;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.data
  (:require
   [clojure.string :as str]
   [synthigy.dataset.id :as id]
   [synthigy.embedded :as embedded]
   [synthigy.iam.access :as access]
   [synthigy.iam.gen :as gen]
   [synthigy.iam.keys]
   [synthigy.iam.transfer :as transfer]
   [synthigy.json :as json]
   [synthigy.oauth.core :as oauth]
   [synthigy.oauth.federated.registry :as registry]))

(defn denied->nil
  [thunk]
  (try (thunk)
       (catch clojure.lang.ExceptionInfo e
         (when-not (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
           (throw e)))))

(def page-size
  100)

(defn visible?
  [{:keys [key]}]
  (try (access/entity-allows? (id/entity key) #{:read})
       (catch Exception _ false)))

(def system-role
  "System Operator")

(defn system-operator?
  "Whether the current principal may see and drive the System page."
  []
  (boolean
   (or (access/superuser?)
       (some #(= system-role (:name %)) (vals (:roles access/*principal*))))))

(defn table-params
  [{:keys [table]} offset limit q rels sort-k dir]
  (let [{:keys [chips sortable]} table]
    (cond-> {"limit" limit "offset" offset}
      (not (str/blank? q))
      (assoc "q" (str "%" (str/trim q) "%"))
      (and sort-k (get sortable (name sort-k)))
      (assoc "sort" (str (name sort-k) " " (if (= "desc" dir) "desc" "asc")))
      :always
      (into (for [[rel-k param] chips
                  :let [xids (seq (get rels rel-k))]
                  :when xids]
              [param (vec xids)])))))

(defn browse-rows
  "One page of `spec`'s rows via its generated op, or nil when RBAC denies."
  ([spec] (browse-rows spec 0 nil nil nil nil))
  ([spec offset q rels sort-k dir]
   (let [{:keys [query]} (:table spec)]
     (denied->nil #(query (table-params spec offset page-size q rels sort-k dir))))))

(def ^:private entity-key
  {:user :iam/user
   :user_group :iam/user-group
   :user_role :iam/user-role
   :oauth_client :iam/app
   :oauth_api :iam/api
   :oauth_scope :iam/scope})

(def ^:private has-active?
  (memoize
   (fn [link-entity]
     (boolean
      (some-> (entity-key link-entity) id/entity
              (as-> want
                    (some #(when (= want (:xid %)) %)
                          (vals (:entities (access/with-principal nil (embedded/schema))))))
              :attributes (get "active"))))))

(defn link-selection
  ([link-entity] (link-selection link-entity nil))
  ([link-entity {:keys [tag sub flag]}]
   (cond-> [:xid :name]
     (has-active? link-entity) (conj :active)
     tag (conj {tag [:xid :name]})
     sub (conj sub)
     flag (conj flag))))

(defn normalize-active
  [rows]
  (mapv #(update % :active (fnil identity true)) rows))

(defn detail-selection
  [{{:keys [fields links settings config]} :detail extra :selection-extra}]
  (into (into (cond-> [:xid]
                settings (conj :settings)
                config (into [(:attr config) (:by config)])
                extra (into extra))
              (map first) fields)
        (map (fn [[k _ entity _ _ show]] {k (link-selection entity show)}))
        links))

(defn relative-path?
  [s]
  (and (str/starts-with? s "/") (not (str/includes? s ".."))))

(defn settings-patch
  [{settings-fields :fields} current params]
  (reduce
   (fn [settings [k label kind]]
     (let [pk (keyword (str "settings__" (name k)))
           v  (get params pk)]
       (assoc settings (name k)
              (case kind
                :switch   (boolean v)
                :uri-list (into [] (comp (map str/trim) (remove str/blank?))
                                (str/split-lines (str v)))
                :grants   (vec (cond (nil? v) [] (sequential? v) v :else [v]))
                :expiry   (into {}
                                (for [sub ["access" "refresh" "id"]
                                      :let [n (some-> (get params (keyword (str (name pk) "__" sub)))
                                                      not-empty parse-long)]
                                      :when n]
                                  [sub n]))
                :relative-path
                (let [v (not-empty (str/trim (str v)))]
                  (when (and v (not (relative-path? v)))
                    (throw (ex-info
                            (str label " must be a relative path starting with "
                                 "\"/\", with no \"..\" segments.")
                            {})))
                  v)
                (not-empty (str v))))))
   (or current {})
   settings-fields))

(defn config-value
  [{:keys [attr string?]} row]
  (let [v (get row attr)
        m (if string?
            (when-not (str/blank? (str v))
              (try (json/read-str-raw (str v)) (catch Exception _ nil)))
            v)]
    (into {}
          (map (fn [[k v]] [(str/replace (name k) "_" "-") v]))
          (or m {}))))

(defn config-write
  [{:keys [by layouts string?] :as config} current params]
  (let [fields  (get layouts (some-> (get current by) name))
        patched (reduce
                 (fn [cfg [k _ kind]]
                   (let [pk (keyword (str "config__" (name k)))
                         v  (get params pk)
                         kn (name k)]
                     (case kind
                       :password (if-let [nv (not-empty (str (or v "")))]
                                   (assoc cfg kn nv)
                                   cfg)
                       :number   (if-let [n (some-> v str not-empty parse-long)]
                                   (assoc cfg kn n)
                                   (dissoc cfg kn))
                       :switch   (assoc cfg kn (boolean v))
                       (if-let [nv (not-empty (str (or v "")))]
                         (assoc cfg kn nv)
                         (dissoc cfg kn)))))
                 (config-value config current)
                 (or fields []))]
    (if string? (json/->json patched) patched)))

(defn detail-row
  [spec xid]
  (when-not (str/blank? xid)
    (some-> (denied->nil
             #(first (embedded/search (:entity spec) {:_where {:xid {:_eq xid}}}
                                      (detail-selection spec))))
            (as-> row
                  (reduce (fn [r [k]] (update r k normalize-active))
                          row (get-in spec [:detail :links]))))))

(def option-limit
  40)

(defn link-options
  ([link-entity q] (link-options link-entity q 0 nil))
  ([link-entity q offset] (link-options link-entity q offset nil))
  ([link-entity q offset show]
   (vec
    (normalize-active
     (denied->nil
      #(embedded/search link-entity
                        (cond-> {:_order_by {:name :asc} :_limit option-limit}
                          (pos? offset) (assoc :_offset offset)
                          (not (str/blank? q))
                          (assoc :_where {:name {:_ilike (str "%" (str/trim q) "%")}}))
                        (link-selection link-entity show)))))))

(defn link-default
  "The row named `nm` for a link's pre-selected default, or nil."
  [link-entity nm]
  (when nm
    (some #(when (= nm (:name %)) %) (link-options link-entity nm))))

(defn link-selected
  ([link-entity xids] (link-selected link-entity xids nil))
  ([link-entity xids show]
   (let [xids (remove str/blank? xids)]
     (when (seq xids)
       (->> (denied->nil #(embedded/search link-entity
                                           {:_where {:xid {:_in (vec xids)}}}
                                           (link-selection link-entity show)))
            normalize-active
            (sort-by :name)
            vec)))))

(defn link-field-data
  [create-fields params]
  (into {}
        (map (fn [[k _ kind]]
               [k (case kind
                    :switch (boolean (get params k))
                    (not-empty (get params k)))]))
        create-fields))

(defn sync-link!
  [link-entity data]
  (try
    [:ok (first (normalize-active [(embedded/sync link-entity data :returning true)]))]
    (catch clojure.lang.ExceptionInfo e
      (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
        [:denied]
        [:error (ex-message e)]))))

(defn create-link!
  "Create an owned child and link it back to `parent-xid` in one sync call."
  [link-entity create-fields params parent parent-xid]
  (let [data (cond-> (link-field-data create-fields params)
               parent (assoc parent {:xid parent-xid}))]
    (if (str/blank? (:name data))
      [:error "Name is required."]
      (sync-link! link-entity data))))

(defn update-link!
  [link-entity create-fields xid params]
  (if (str/blank? xid)
    [:error "Nothing selected."]
    (let [data (assoc (link-field-data create-fields params) :xid xid)]
      (if (str/blank? (:name data))
        [:error "Name is required."]
        (sync-link! link-entity data)))))

(defn delete-link!
  [link-entity xid]
  (if (str/blank? xid)
    [:error "Nothing selected."]
    (try
      (embedded/purge link-entity {:_where {:xid {:_eq xid}}} [:xid])
      [:ok xid]
      (catch clojure.lang.ExceptionInfo e
        (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
          [:denied]
          [:error (ex-message e)])))))

(defn enum-values
  [{:keys [key]} attr]
  (access/with-principal nil
    (let [want   (id/entity key)
          entity (some #(when (= want (:xid %)) %)
                      (vals (:entities (embedded/schema))))]
      (get-in entity [:attributes (name attr) :enum]))))

(defn all-grants
  [spec xid link-key]
  (access/with-principal nil
    (-> (embedded/search (:entity spec) {:_where {:xid {:_eq xid}}}
                         [:xid {link-key [:xid]}])
        first
        (get link-key))))

(defn link-payload
  [spec xid link-key link-entity submitted]
  (let [current   (mapv :xid (all-grants spec xid link-key))
        seen-cur  (set (map :xid (link-selected link-entity current)))
        grantable (set (map :xid (link-selected link-entity submitted)))]
    (into (mapv (fn [x] {:xid x}) (remove seen-cur current))
          (comp (filter grantable) (map (fn [x] {:xid x})))
          submitted)))

(defn create-link-payload
  [link-entity submitted]
  (mapv (fn [{:keys [xid]}] {:xid xid}) (link-selected link-entity submitted)))

(defn listy
  [v]
  (cond (nil? v) [] (sequential? v) (vec v) :else [v]))

(defn panel-rows
  "An owned-child list as the FORM carries it — parallel `<k>__*` vectors, zipped by index."
  [link-key create-fields params]
  (let [cols (into {:xid (listy (get params (keyword (str (name link-key) "__xid"))))}
                   (map (fn [[fk]]
                          [fk (listy (get params (keyword (str (name link-key)
                                                               "__" (name fk)))))]))
                   create-fields)
        n    (apply max 0 (map count (vals cols)))]
    (into []
          (keep (fn [i]
                  (let [row (into {} (map (fn [[fk vs]] [fk (nth vs i nil)])) cols)]
                    (when-not (str/blank? (str (:name row))) row))))
          (range n))))

(defn panel-deleted
  "xids of saved rows the user removed — `save!` must purge these or they orphan."
  [link-key params]
  (into [] (remove str/blank?)
        (listy (get params (keyword (str (name link-key) "__deleted"))))))

(defn owned-payload
  "Form rows to nested sync payload; a row with an `:xid` updates, one without is created and linked."
  [create-fields rows]
  (mapv (fn [row]
          (into (if (str/blank? (str (:xid row))) {} {:xid (:xid row)})
                (keep (fn [[fk _ kind]]
                        (let [v (get row fk)]
                          (case kind
                            :switch [fk (= "true" (str v))]
                            (when-not (str/blank? (str v)) [fk (str v)])))))
                create-fields))
        rows))

(defn owned-links
  [links]
  (filterv #(get-in (last %) [:create :parent]) links))

(defn owned-data
  "Nested owned-child payloads. A link's own `:create :validate` runs here and
   THROWS — `save!` turns any non-ENTITY_FORBIDDEN exception into an `[:error]`
   notice, the same path a bad `login-page` takes, so a rejected value keeps
   the typed form on screen instead of writing."
  [links params]
  (into {}
        (map (fn [[k _ _ _ _ show]]
               (let [cf   (get-in show [:create :fields])
                     rows (panel-rows k cf params)]
                 (when-let [validate (get-in show [:create :validate])]
                   (doseq [row rows]
                     (when-let [bad (validate row)]
                       (throw (ex-info bad {})))))
                 [k (owned-payload cf rows)])))
        (owned-links links)))

(defn purge-deleted!
  [links params]
  (doseq [[k _ link-entity] (owned-links links)
          x (panel-deleted k params)]
    (try (embedded/purge link-entity {:_where {:xid {:_eq x}}} [:xid])
         (catch clojure.lang.ExceptionInfo _ nil))))

(defn scalar-params
  "The spec's own scalar fields, coerced — the part of a save that CANNOT throw.
   Hoisted out of `save!` so a rejected write can re-render what the person
   typed instead of re-reading the stored row and silently reverting it. Raw
   params won't do: an unchecked `ty-switch` posts nothing, so merging them
   would leave a switch reading `true` after the user turned it off."
  [fields params]
  (into {}
        (map (fn [[k _ kind]]
               [k (case kind
                    (:switch :flag) (boolean (get params k))
                    :enum   (some-> (get params k) not-empty keyword)
                    :number (some-> (get params k) str not-empty parse-long)
                    (get params k))]))
        fields))

(defn save!
  "`[:ok row]`, or `[:denied typed]` / `[:error msg typed]` where `typed` is
   what was posted, for the caller to re-render."
  [{{:keys [fields links settings config prepare]} :detail :as spec} xid params]
  (if-let [current (detail-row spec xid)]
    (let [typed (scalar-params fields params)]
      (try
        (let [data (assoc typed :xid xid)
              data (into data
                         (comp
                          (remove (fn [l] (get-in (last l) [:create :parent])))
                          (map (fn [[k _ link-entity]]
                                 [k (link-payload spec xid k link-entity
                                                  (get params k))])))
                         links)
              data (merge data (owned-data links params))
              data (cond-> data
                     settings (assoc :settings
                                     (settings-patch settings (:settings current) params))
                     (and config
                          (some (fn [[k]] (str/starts-with? (name k) "config__")) params))
                     (assoc (:attr config) (config-write config current params)))]
          (embedded/sync (:entity spec) (cond-> data prepare (prepare current)))
          (purge-deleted! links params)
          [:ok (detail-row spec xid)])
        (catch clojure.lang.ExceptionInfo e
          (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
            [:denied typed]
            [:error (ex-message e) typed]))))
    [:denied]))

(defn delete-row!
  "Delete a record and the children it OWNS, children first."
  [{:keys [entity] {:keys [links]} :detail :as spec} xid]
  (if-let [row (detail-row spec xid)]
    (try
      (doseq [[k _ link-entity] (owned-links links)
              :let [xids (mapv :xid (get row k))]
              :when (seq xids)]
        (embedded/purge link-entity {:_where {:xid {:_in xids}}} [:xid]))
      (embedded/purge entity {:_where {:xid {:_eq xid}}} [:xid])
      [:ok row]
      (catch clojure.lang.ExceptionInfo e
        (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
          [:denied]
          [:error (ex-message e)])))
    [:denied]))

(defn regenerate-secret!
  [spec xid]
  (if (detail-row spec xid)
    (try
      (let [secret (gen/client-secret)]
        (embedded/sync (:entity spec) {:xid xid :secret secret})
        [:ok secret])
      (catch clojure.lang.ExceptionInfo e
        (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
          [:denied]
          [:error (ex-message e)])))
    [:denied]))

(defn create-row!
  [{create :create detail :detail :as spec} params]
  (let [data    (into {}
                      (map (fn [[k _ kind {:keys [keyword?]}]]
                             [k (case kind
                                  :switch  (boolean (get params k))
                                  :number  (some-> (get params k) str not-empty parse-long)
                                  :choices (let [v (not-empty (str (or (get params k) "")))]
                                             (if (and v keyword?) (keyword v) v))
                                  (not-empty (str (or (get params k) ""))))]))
                      (:fields create))
        data    (into data
                      (map (fn [[k _ link-entity]]
                             [k (create-link-payload link-entity (get params k))]))
                      (:links create))
        data    (merge data (owned-data (:owned create) params))
        data    (cond-> data
                  (:settings detail)
                  (assoc :settings (settings-patch (:settings detail) nil params))
                  (:config detail)
                  (assoc (get-in detail [:config :attr]) (config-write (:config detail) data params)))
        missing (some (fn [[k label _ opts]]
                        (when (and (:required opts) (nil? (get data k))) label))
                      (:fields create))]
    (if missing
      [:error (str missing " is required.")]
      (try
        ;; `:prepare` mints server-owned values (an OAuth client_id) into the
        ;; payload at WRITE time, the way the dataset layer mints xid. Never a
        ;; hidden form field: `oauth_client` has UNIQUE(id), which makes `id`
        ;; sync's MATCH KEY — a posted id that already exists UPDATES that row
        ;; instead of creating one, so a client-supplied value turns this form
        ;; into "overwrite any client".
        [:ok (embedded/sync (:entity spec)
                            (cond-> data
                              (:prepare create) ((:prepare create)))
                            :returning true)]
        (catch clojure.lang.ExceptionInfo e
          (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
            [:denied]
            [:error (ex-message e)]))))))

(defn set-active!
  "Flip one row's `:active`, reporting whether the write landed."
  [spec xid active?]
  (try
    (embedded/sync (:entity spec) {:xid xid :active active?})
    true
    (catch clojure.lang.ExceptionInfo _ false)))

(def transfer-types
  [{:type :role  :slug "roles"  :key :iam/user-role}
   {:type :group :slug "groups" :key :iam/user-group}
   {:type :user  :slug "users"  :key :iam/user}
   {:type :app   :slug "apps"   :key :iam/app}
   {:type :api   :slug "apis"   :key :iam/api}])

(defn transfer-type [t]
  (some #(when (= t (:type %)) %) transfer-types))

(defn transfer-by-slug [slug]
  (some #(when (= slug (:slug %)) %) transfer-types))

(defn transfer-exportable? [t]
  (boolean
   (when-let [k (:key (transfer-type t))]
     (try (access/entity-allows? (id/entity k) #{:browse})
          (catch Exception _ false)))))

(defn transfer-importable? [t]
  (boolean
   (when-let [k (:key (transfer-type t))]
     (try (access/entity-allows? (id/entity k) #{:write})
          (catch Exception _ false)))))

(defn transfer-export
  ([t] (transfer-export t nil))
  ([t ids]
   (if (seq ids)
     (transfer/export t {:xid {:_in (vec ids)}})
     (transfer/export t))))

(defn transfer-validate [t payload]
  (try
    (let [{:keys [records missing]} (transfer/validate-records t payload)]
      [:ok {:records records :missing missing}])
    (catch Exception e
      [:error (ex-message e)])))

(defn transfer-import! [t payload mode]
  (try
    (transfer/import-records! t payload :mode mode :source "console")
    [:ok (count (if (sequential? payload) payload [payload]))]
    (catch clojure.lang.ExceptionInfo e
      (if-let [missing (:missing (ex-data e))]
        [:missing missing]
        [:error (ex-message e)]))
    (catch Exception e
      [:error (ex-message e)])))

(def ^:private session-selection
  [:xid :id :active :started :authorized-at :last-seen :context
   {:client {:selections [:xid :name]}}])

(defn owned-by
  [user-xid]
  {:user {:selections [:xid]
          :args {:_join :inner :_where {:xid {:_eq user-xid}}}}})

(defn my-sessions
  [user-xid current-id]
  (->> (access/with-principal nil
         (embedded/search :oauth_session
                          {:_where {:active {:_eq true}}}
                          (conj session-selection (owned-by user-xid))))
       (sort-by :started #(compare %2 %1))
       (mapv (fn [{:keys [id] :as s}]
               (-> s
                   (assoc :current? (and (some? id) (= id current-id)))
                   (dissoc :id))))))

(def ^:private identity-selection
  [:xid :provider :email :linked-at])

(defn my-identities
  [user-xid]
  (->> (access/with-principal nil
         (embedded/search :external_identity nil
                          (conj identity-selection
                                {:user {:selections [:xid]
                                        :args {:_join :inner
                                               :_where {:xid {:_eq user-xid}}}}})))
       (sort-by :linked-at #(compare %2 %1))
       vec))

(defn provider-split
  "Active federation providers as `{:linked [..] :available [..]}` for `user-xid`."
  [user-xid]
  (let [linked (into #{} (map (comp registry/provider-keyword :provider)) (my-identities user-xid))
        all    (access/with-principal nil (registry/list-providers))]
    (group-by #(if (linked (:provider %)) :linked :available) all)))

(defn has-password?
  "Whether `user-xid` has a local password set; an LDAP/webhook connector can
   still authenticate a user this reports false for."
  [user-xid]
  (some? (access/with-principal nil
           (:password (embedded/get :user {:xid user-xid} [:password])))))

(defn kill-my-session!
  [user-xid session-xid]
  (boolean
   (when-not (str/blank? session-xid)
     (when-let [{sid :id} (first (access/with-principal nil
                                   (embedded/search :oauth_session
                                                    {:_where {:active {:_eq true}
                                                              :xid {:_eq session-xid}}}
                                                    (conj session-selection
                                                          (owned-by user-xid)))))]
       ;; SYSTEM: kill-session touches :oauth/session; ownership decided above
       (access/with-principal nil (oauth/kill-session sid))
       true))))

(defn set-password!
  "Set `user-xid`'s password. SYSTEM write — the caller must already have proven
   presence; `user-xid` must come from the session row, NEVER from a parameter."
  [user-xid password]
  ;; plaintext — the dataset layer derives `hashed` attributes on write
  (try
    (access/with-principal nil
      (embedded/sync :user {:xid user-xid :password password}))
    [:ok]
    (catch clojure.lang.ExceptionInfo e
      (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
        [:denied]
        [:error (ex-message e)]))))

(defn kill-other-sessions!
  "Sign `user-xid` out everywhere except the session holding `current-id`.
   Returns how many were killed."
  [user-xid current-id]
  (let [others (->> (access/with-principal nil
                      (embedded/search :oauth_session
                                       {:_where {:active {:_eq true}}}
                                       (conj session-selection (owned-by user-xid))))
                    (remove #(= current-id (:id %))))]
    (access/with-principal nil
      (doseq [{sid :id} others]
        (oauth/kill-session sid)))
    (count others)))

(def public-profile-fields
  [[:name "Name" :text]
   [:given_name "Given name" :text]
   [:family_name "Family name" :text]
   [:nickname "Nickname" :text]
   [:preferred_username "Preferred username" :text]
   [:profile "Profile URL" :text]
   [:picture "Picture URL" :text]
   [:website "Website" :text]
   [:zone_info "Time zone" :text]])

(def person-info-fields
  [[:email "Email" :text]
   [:phone_number "Phone number" :text]
   [:gender "Gender" :enum]
   [:birthdate "Birthdate" :text]
   [:middle_name "Middle name" :text]
   [:address "Address" :textarea]])

(defn get-profile
  [user-xid]
  (let [{:keys [person_info public_profile]}
        (first (access/with-principal nil
                 (embedded/search :user {:_where {:xid {:_eq user-xid}}}
                                  [:xid
                                   {:person_info [{:args {:_join :left}
                                                   :selections (mapv first person-info-fields)}]}
                                   {:public_profile [{:args {:_join :left}
                                                      :selections (mapv first public-profile-fields)}]}]
                                  :key-format "snake")))]
    (merge person_info public_profile)))

(defn save-profile!
  [user-xid params]
  (try
    (access/with-principal nil
      (embedded/sync :user
                     {:xid user-xid
                      :person_info (into {} (map (fn [[k]] [k (get params k)])) person-info-fields)
                      :public_profile (into {} (map (fn [[k]] [k (get params k)])) public-profile-fields)}))
    [:ok (get-profile user-xid)]
    (catch clojure.lang.ExceptionInfo e
      (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
        [:denied]
        [:error (ex-message e)]))))
