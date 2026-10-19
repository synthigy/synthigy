;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

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
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.custom :as custom]))

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

(defn link-via
  [[_ _ _ _ _ show]]
  (:via show))

(defn detail-selection
  [{{:keys [fields links settings config]} :detail extra :selection-extra}]
  (let [link-sel (fn [[k _ entity _ _ show]] {k (link-selection entity show)})]
    (-> (cond-> [:xid]
          settings (conj :settings)
          config (into [(:attr config) (:by config)])
          extra (into extra))
        (into (map first) fields)
        (into (map link-sel) (remove link-via links))
        (into (map (fn [[rel ls]]
                       {rel [{:args {:_join :left}
                              :selections (into [:xid]
                                                (map (fn [[k _ entity _ _ show]]
                                                       {k [{:args {:_join :left}
                                                            :selections (link-selection entity show)}]}))
                                                ls)}]}))
              (group-by (comp first link-via) (filter link-via links))))))

(defn listy
  [v]
  (cond (nil? v) [] (sequential? v) (vec v) :else [v]))

(defn setting-posted
  "One declared setting as the form posted it; `:grants` keeps only the values it offers."
  [[k _ kind _ opts] params]
  (let [pk (keyword (str "settings__" (name k)))
        v  (get params pk)]
    (case kind
      :switch        (boolean v)
      :uri-list      (into [] (comp (map str/trim) (remove str/blank?))
                           (str/split-lines (str v)))
      :grants        (let [known (set (map first (:choices opts)))]
                       (filterv known (listy v)))
      :expiry        (into {}
                           (for [sub ["access" "refresh" "id"]
                                 :let [n (some-> (get params (keyword (str (name pk) "__" sub)))
                                                 not-empty parse-long)]
                                 :when n]
                             [sub n]))
      (:login-page :url) (not-empty (str/trim (str v)))
      (not-empty (str v)))))

(defn check-setting!
  [[_ label kind] v]
  (when (and (= :login-page kind) v (not (custom/valid-name? v)))
    (throw (ex-info (str label " must name a login page bundle — lowercase letters, "
                         "digits and dashes.")
                    {})))
  (when (and (= :url kind) v (not (re-matches #"https?://[^\s/]+(/\S*)?" v)))
    (throw (ex-info (str label " must be a full http(s) address, e.g. https://app.example.com/.")
                    {}))))

(defn settings-value
  "A fresh `:settings` map from the posted form — create only, where nothing is stored yet."
  [{fields :fields} params]
  (into {}
        (map (fn [[k :as f]]
               (let [v (setting-posted f params)]
                 (check-setting! f v)
                 [(name k) v])))
        fields))

(defn same-setting?
  [kind old new]
  (case kind
    :switch (= (boolean old) new)
    :grants (= (set (listy old)) (set new))
    (:uri-list :expiry) (= (or (not-empty old) (empty new)) new)
    (= (not-empty (str (or old ""))) new)))

(defn settings-patch
  "`current` with each declared setting that DIFFERS from what was posted replaced — undeclared keys and grant values the form doesn't offer survive."
  [{fields :fields} current params]
  (reduce
   (fn [settings [k _ kind _ opts :as f]]
     (let [kn    (name k)
           old   (get settings kn)
           new   (setting-posted f params)
           known (set (map first (:choices opts)))]
       (cond
         (= :grants kind)
         (if (= (set (filter known (listy old))) (set new))
           settings
           (assoc settings kn (into (filterv (complement known) (listy old)) new)))

         (same-setting? kind old new) settings

         :else (do (check-setting! f new)
                   (assoc settings kn new)))))
   (or current {})
   fields))

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

(defn config-encode
  [{:keys [string?]} cfg]
  (if string? (json/->json cfg) cfg))

(defn config-fields
  [{:keys [by layouts]} row]
  (get layouts (some-> (get row by) name)))

(defn config-merge
  "The stored config with the posted `config__*` fields applied — password blank keeps, other blanks drop."
  [config current params]
  (reduce
   (fn [cfg [k _ kind]]
     (let [v  (get params (keyword (str "config__" (name k))))
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
   (or (config-fields config current) [])))

(defn config-write
  [config current params]
  (config-encode config (config-merge config current params)))

(defn shape-detail-row
  "A searched row as the editor reads it — via links flattened onto their own keys, `:active` on every link row."
  [spec row]
  (when row
    (reduce (fn [r [k :as l]]
              (let [r (if-let [[rel] (link-via l)]
                        (assoc r k (get-in r [rel k]))
                        r)]
                (update r k normalize-active)))
            row (get-in spec [:detail :links]))))

(defn detail-row
  [spec xid]
  (when-not (str/blank? xid)
    (shape-detail-row
     spec
     (denied->nil
      #(first (embedded/search (:entity spec) {:_where {:xid {:_eq xid}}}
                               (detail-selection spec)))))))

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
  [entity xid link-key]
  (access/with-principal nil
    (-> (embedded/search entity {:_where {:xid {:_eq xid}}}
                         [:xid {link-key [:xid]}])
        first
        (get link-key))))

(defn link-payload
  "The grant set to write: current grants the principal cannot see survive, submitted ones are kept only when grantable."
  [current link-entity submitted]
  (let [seen-cur  (set (map :xid (link-selected link-entity current)))
        grantable (set (map :xid (link-selected link-entity submitted)))]
    (into (mapv (fn [x] {:xid x}) (remove seen-cur current))
          (comp (filter grantable) (map (fn [x] {:xid x})))
          submitted)))

(defn create-link-payload
  [link-entity submitted]
  (mapv (fn [{:keys [xid]}] {:xid xid}) (link-selected link-entity submitted)))

(defn panel-rows
  "An owned-child list as the create FORM carries it — parallel `<k>__*` vectors, zipped by index."
  [link-key create-fields params]
  (let [cols (into {}
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

(defn owned-payload
  [create-fields rows]
  (mapv (fn [row]
          (into {}
                (keep (fn [[fk _ kind]]
                        (let [v (get row fk)]
                          (case kind
                            :switch [fk (= "true" (str v))]
                            (when-not (str/blank? (str v)) [fk (str v)])))))
                create-fields))
        rows))

(defn owned?
  [link]
  (some? (get-in (last link) [:create :parent])))

(defn owned-links
  [links]
  (filterv owned? links))

(defn owned-data
  "Nested owned-child payloads for a create; a link's `:create :validate` THROWS on a bad row."
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

(defn field-value
  [[k label kind] params]
  (let [v (get params k)]
    (case kind
      (:switch :flag) (boolean v)
      :enum   (some-> v not-empty keyword)
      :number (when-let [s (not-empty (str/trim (str (or v ""))))]
                (or (parse-long s)
                    (throw (ex-info (str label " must be a whole number.") {}))))
      v)))

(defn same-value?
  [kind old new]
  (case kind
    (:switch :flag) (= (boolean old) (boolean new))
    :enum   (= (some-> old name) (some-> new name))
    :number (if (and (number? old) (number? new)) (== old new) (= old new))
    (= (not-empty (str (or old ""))) (not-empty (str (or new ""))))))

(defn unit
  "The detail unit named `nm`: `[:field f]`, `[:link l]`, `[:settings s]` or `[:config c]`, or nil."
  [{{:keys [fields links settings config]} :detail} nm]
  (let [named (fn [xs] (some #(when (= nm (name (first %))) %) xs))]
    (cond
      (and settings (= "settings" nm)) [:settings settings]
      (and config (= "config" nm))     [:config config]
      (named fields)                   [:field (named fields)]
      (named (remove owned? links))    [:link (named (remove owned? links))])))

(defn controls-under
  "The names after `prefix` among the committed `controls` — `settings__token-expiry__access` under `settings__` is `token-expiry`."
  [controls prefix]
  (into #{}
        (keep #(when (str/starts-with? % prefix)
                 (first (str/split (subs % (count prefix)) #"__|~"))))
        (str/split (str controls) #",")))

(defn only-field
  "The block `fields` narrowed to those whose controls committed — none when no control is named."
  [fields prefix controls]
  (let [ks (controls-under controls (str prefix "__"))]
    (filterv #(ks (name (first %))) fields)))

(defn control-toggles
  "`[[op value] …]` committed for control `nm` — `<nm>~+<v>` adds, `<nm>~-<v>` removes, `<nm>~*` clears."
  [controls nm]
  (keep #(when-let [[_ c op v] (re-matches #"([^~]+)~([+*-])(.*)" %)]
           (when (= nm c) [op v]))
        (str/split (str controls) #",")))

(defn apply-toggles
  [values toggles]
  (reduce (fn [cur [op v]]
            (case op
              "+" (if (some #{v} cur) cur (conj cur v))
              "-" (filterv #(not= v %) cur)
              "*" []))
          (vec values)
          toggles))

(defn toggled-params
  "Checkbox groups rebuilt from what is stored plus the committed toggles — never from the form's own checkbox state."
  [prefix {:keys [fields]} stored params]
  (reduce (fn [ps [k]]
            (let [nm      (str prefix "__" (name k))
                  toggles (control-toggles (:unit/control params) nm)]
              (if (seq toggles)
                (assoc ps (keyword nm) (apply-toggles (listy (get stored (name k))) toggles))
                ps)))
          params
          fields))

(defn link-change
  "The link write for a picker — from the stored links plus its committed add/remove toggles when it sent any, else from the posted set."
  [entity xid k link-entity submitted toggles]
  (let [current   (mapv :xid (all-grants entity xid k))
        submitted (if (seq toggles) (apply-toggles current toggles) submitted)
        payload   (link-payload current link-entity (distinct (remove str/blank? submitted)))]
    (when (not= (set current) (set (map :xid payload)))
      [entity xid {k payload}])))

(defn unit-changes
  "`[entity xid changes]` for the write a posted unit needs, or nil when it matches what is stored."
  [{:keys [entity]} current [kind u] params]
  (case kind
    :field    (let [[k _ fkind] u
                    v (field-value u params)]
                (when-not (same-value? fkind (get current k) v)
                  [entity (:xid current) {k v}]))
    :settings (let [stored (or (:settings current) {})
                    new    (settings-patch (update u :fields only-field "settings" (:unit/control params))
                                           stored (toggled-params "settings" u stored params))]
                (when (not= stored new)
                  [entity (:xid current) {:settings new}]))
    :config   (let [stored (config-value u current)
                    new    (config-merge (update u :layouts update-vals
                                                 #(only-field % "config" (:unit/control params)))
                                         current params)]
                (when (not= stored new)
                  [entity (:xid current) {(:attr u) (config-encode u new)}]))
    :link     (let [[k _ link-entity] u
                    submitted (listy (get params k))
                    toggles   (control-toggles (:unit/control params) (name k))]
                (if-let [[rel via-entity] (link-via u)]
                  (when-let [target (get-in current [rel :xid])]
                    (link-change via-entity target k link-entity submitted toggles))
                  (link-change entity (:xid current) k link-entity submitted toggles)))))

(def unit-locks
  (vec (repeatedly 64 #(Object.))))

(defn row-lock
  [xid]
  (nth unit-locks (mod (hash (str xid)) (count unit-locks))))

(defn editable-row
  [{{:keys [locked]} :detail :as spec} xid]
  (when-let [row (detail-row spec xid)]
    (when-not (and locked (locked row)) row)))

(defn save-unit!
  "Write detail unit `nm` only when it differs from the stored row — `[:unchanged row]`, `[:ok before after]`, `[:confirm msg]`, `[:denied]` or `[:error msg]`."
  [{{:keys [prepare confirm-change]} :detail :keys [entity] :as spec} xid nm params confirmed?]
  (locking (row-lock xid)
    (let [current (editable-row spec xid)
          u       (when current (unit spec nm))]
      (if-not u
        [:denied]
        (try
          (if-let [[target txid changes] (unit-changes spec current u params)]
            (let [main? (= target entity)
                  msg   (when (and main? confirm-change (not confirmed?))
                          (confirm-change changes current))]
              (if msg
                [:confirm msg]
                (do (embedded/sync target (assoc (cond-> changes
                                                   (and main? prepare) (prepare current))
                                                 :xid txid))
                    [:ok current (detail-row spec xid)])))
            [:unchanged current])
          (catch clojure.lang.ExceptionInfo e
            (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
              [:denied]
              [:error (ex-message e)])))))))

(defn child-value
  "An owned child's field whichever casing carried it — rows read back kebab-case, create fields may be declared snake_case."
  [row fk]
  (some #(let [v (get row %)] (when (some? v) v))
        [fk
         (keyword (str/replace (name fk) "_" "-"))
         (keyword (str/replace (name fk) "-" "_"))]))

(defn child-changes
  "The card fields that differ from `child` — a switch only when its control was flipped (`:card/touched`), never from its posted state."
  [fields child card]
  (into {}
        (keep (fn [[fk _ kind]]
                (when (or (not= :switch kind) (contains? (:card/touched card) fk))
                  (let [v (if (= :switch kind) (boolean (get card fk)) (not-empty (get card fk)))]
                    (when-not (same-value? kind (child-value child fk) v)
                      [fk v])))))
        fields))

(defn panel-write!
  "Add, edit or delete one owned child of a stored row — `[:ok]`, `[:unchanged]`, `[:denied]` or `[:error msg]`."
  [row [k _ link-entity _ _ show] op child-xid card]
  (let [{:keys [fields parent validate]} (:create show)
        children (get row k)
        child    (when child-xid (some #(when (= child-xid (:xid %)) %) children))
        nm       (str/trim (str (:name card)))
        bad      (when (#{:add :edit} op)
                   (cond
                     (str/blank? nm) "Name is required."
                     (some #(and (not= child-xid (:xid %)) (= nm (str/trim (str (:name %)))))
                           children)
                     (str "\"" nm "\" is already in the list.")
                     validate (validate (assoc card :name nm))))]
    (cond
      (and (#{:edit :delete} op) (nil? child)) [:error "That item is no longer here."]
      bad                                      [:error bad]
      (= :add op)                              (create-link! link-entity fields (assoc card :name nm)
                                                             parent (:xid row))
      (= :delete op)                           (delete-link! link-entity child-xid)
      :else
      (let [changes (child-changes fields child (assoc card :name nm))]
        (if (empty? changes)
          [:unchanged]
          (sync-link! link-entity (assoc changes :xid child-xid)))))))

(defn delete-row!
  "Delete a record and the children it OWNS, children first."
  [{:keys [entity] {:keys [links]} :detail :as spec} xid]
  (if-let [row (editable-row spec xid)]
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
  (try
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
                    (assoc :settings (settings-value (:settings detail) params))
                    (:config detail)
                    (assoc (get-in detail [:config :attr]) (config-write (:config detail) data params)))
          missing (some (fn [[k label _ opts]]
                          (when (and (:required opts) (nil? (get data k))) label))
                        (:fields create))]
      (if missing
        [:error (str missing " is required.")]
        ;; never take server-owned values (client_id) from the form — UNIQUE(id) makes a posted id overwrite that client
        [:ok (embedded/sync (:entity spec)
                            (cond-> data
                              (:prepare create) ((:prepare create)))
                            :returning true)]))
    (catch clojure.lang.ExceptionInfo e
      (if (= "ENTITY_FORBIDDEN" (:code (ex-data e)))
        [:denied]
        [:error (ex-message e)]))))

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
  "Whether `user-xid` has a local password set."
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
  "SYSTEM write of `user-xid`'s password; `user-xid` must come from the session row, never a parameter."
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
  "Sign `user-xid` out everywhere except the session holding `current-id`, returning the count."
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

(defn profile-field
  [nm]
  (or (some #(when (= nm (name (first %))) [:public_profile %]) public-profile-fields)
      (some #(when (= nm (name (first %))) [:person_info %]) person-info-fields)))

(defn save-profile-field!
  "SYSTEM write of one of `user-xid`'s profile fields when it differs; `user-xid` must come from the session."
  [user-xid nm params]
  (when-let [[rel [k _ kind :as f]] (profile-field nm)]
    (try
      (let [before (get-profile user-xid)
            v      (field-value f params)]
        (if (same-value? kind (get before k) v)
          [:unchanged before]
          (do (access/with-principal nil
                (embedded/sync :user {:xid user-xid rel {k v}}))
              [:ok before (get-profile user-xid)])))
      (catch clojure.lang.ExceptionInfo e
        [:error (ex-message e)]))))
