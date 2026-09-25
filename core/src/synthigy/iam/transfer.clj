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

(ns synthigy.iam.transfer
  "Export/import portable IAM configuration as id-keyed JSON. `export` runs in
   SYSTEM context and produces an unfiltered file — anything exposing it over
   HTTP must gate on ROOT (synthigy.iam.access/superuser?), never RBAC-project
   it, see synthigy.server.data/iam-export-handler."
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [jsonista.core :as j]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :refer [normalize-name]]
   [synthigy.dataset.sql.query :as sql.query]
   [synthigy.iam.access :as access]
   [synthigy.iam.gen :as gen]
   [synthigy.iam.service-user :as service-user]
   synthigy.iam.keys
   [synthigy.json :refer [read-str]]
   [synthigy.log :as log]))

;; ============================================================================
;; What is transferable
;; ============================================================================

(def transferable
  "type -> {:entity, :prefix, :scalars, :refs}; :refs maps a relation key to :ref (id-only stub) or a nested {:scalars #{…}} when this export IS the target's definition."
  {:role  {:entity  :iam/user-role
           :prefix  "role_"
           :scalars #{:name :description :active :avatar}
           :refs    (zipmap [:scopes
                             ;; CRUDOB — dropping :browse_entities strips B from
                             ;; every exported role on import
                             :create_entities :read_entities :update_entities
                             :delete_entities :owned_entities :browse_entities
                             :from_read_relations :to_read_relations
                             :from_write_relations :to_write_relations
                             :from_delete_relations :to_delete_relations
                             ;; dropping the deny-lists would SILENTLY WIDEN
                             ;; access on import
                             :deny_read_attributes :deny_write_attributes]
                            (repeat :ref))}

   :group {:entity  :iam/user-group
           :prefix  "group_"
           :scalars #{:name :description :active :type :avatar}
           ;; dropping :datasets strips the RLS dataset scope from every
           ;; exported group on import
           :refs    {:datasets :ref}}

   :user  {:entity  :iam/user
           :prefix  "user_"
           ;; NO :password (hashed), NO :person_info/:public_profile (own opt-in
           ;; export)
           :scalars #{:name :active :type :avatar :priority :settings}
           :refs    {:roles :ref :groups :ref}}

   :app   {:entity  :iam/app
           :prefix  "app_"
           ;; NO :secret (hashed)
           :scalars #{:name :description :id :active :type :settings}
           :refs    {:apis :ref}
           ;; roles/groups are the app's, stored on its SERVICE user
           :via     {:service_user {:roles :ref :groups :ref}}}

   :api   {:entity  :iam/api
           :prefix  "api_"
           :scalars #{:name :description :audience :avatar :configuration}
           :refs    {:scopes {:scalars #{:name :description}}}}})

(defn spec [type]
  (or (get transferable type)
      (throw (ex-info (str "Not a transferable IAM type: " type)
                      {:type type :known (vec (sort (keys transferable)))}))))

;; An unknown selection key THROWS, so every key below is intersected against
;; the live deployed model before use — narrowing never widens the allowlist.

(defn live-attributes
  "ACTIVE attribute key -> type string for a deployed entity, by resolved id."
  [entity-id]
  (into {}
        (comp (filter :active)
              (map (juxt (comp keyword normalize-name :name) :type)))
        (:attributes (dataset/deployed-entity entity-id))))

(defn entity-relations
  "`relation-selection-key -> relation` for a deployed entity, or nil."
  [entity-id]
  (let [eid (str entity-id)]
    (some (fn [[k ent]] (when (= (str k) eid) (:relations ent)))
          (sql.query/model->schema (dataset/deployed-model)))))

(defn assert-no-secrets!
  "Throw if the allowlist names an attribute the model stores as `hashed` or
   `encrypted` — never trust a hardcoded field-name denylist here."
  [type entity attrs scalars]
  (when-let [leaks (seq (filter #(#{"hashed" "encrypted"} (get attrs %)) scalars))]
    (throw (ex-info (str "Refusing to export credential fields from " entity)
                    {:type type :entity entity
                     :fields (vec (sort leaks))
                     :types  (select-keys attrs leaks)}))))

(defn selection
  "The `/data` selection this type exports, narrowed to the live model."
  [type]
  (let [{:keys [entity scalars refs]} (spec type)
        entity-id (id/entity entity)
        attrs     (live-attributes entity-id)
        rels      (entity-relations entity-id)
        scalars'  (filter attrs scalars)]
    (assert-no-secrets! type entity attrs scalars')
    (into {(id/key) nil}
          (concat
           (map (fn [k] [k nil]) scalars')
           (keep (fn [[rel-key rel-spec]]
                   (when-let [rel (get rels rel-key)]
                     [rel-key
                      [{:args {:_join :left}
                        :selections
                        (if (= :ref rel-spec)
                          {(id/key) nil}
                          ;; `:to` IS the target entity id — narrow against ITS
                          ;; attributes, not the parent's
                          (let [target-attrs (live-attributes (:to rel))]
                            (into {(id/key) nil}
                                  (map (fn [k] [k nil]))
                                  (filter target-attrs (:scalars rel-spec)))))}]]))
                 refs)
           (keep (fn [[via-key via-refs]]
                   (when-let [rel (get rels via-key)]
                     (let [target-rels (entity-relations (:to rel))]
                       [via-key
                        [{:args {:_join :left}
                          :selections
                          (into {(id/key) nil}
                                (keep (fn [[k _]]
                                        (when (get target-rels k)
                                          [k [{:args {:_join :left}
                                               :selections {(id/key) nil}}]])))
                                via-refs)}]])))
                 (:via (spec type)))))))

(defn lift-via
  "Move a record's via-relation refs up to the record itself."
  [type record]
  (reduce (fn [r [via-key via-refs]]
            (merge (dissoc r via-key)
                   (select-keys (get r via-key) (keys via-refs))))
          record
          (:via (spec type))))

;; ============================================================================
;; Export
;; ============================================================================

(defn portable
  "Drop `_eid` and nil-valued keys at every depth; sort keys and sort
   collections of records by id (scalar collections keep their stored order)."
  [x]
  (cond
    (map? x)
    (into (sorted-map)
          (keep (fn [[k v]]
                  (when-not (or (= :_eid k) (nil? v))
                    [k (portable v)])))
          x)

    (sequential? x)
    (let [vs (mapv portable x)]
      (if (every? map? vs) (vec (sort-by (id/key) vs)) vs))

    :else x))

(defn export
  "Portable records for `type`, deterministically ordered. `args` is an optional
   `/data` predicate in OPERATOR form (`{:name {:_eq \"IAM Admin\"}}`) — a bare
   `{:name \"IAM Admin\"}` fails deep in the SQL builder."
  ([type] (export type nil))
  ([type args]
   (let [{:keys [entity]} (spec type)
         sel (selection type)]
     (access/with-principal nil
       (let [records (mapv (comp portable #(lift-via type %))
                           (dataset/search-entity entity args sel))]
         (log/info {:id ::exported
                    :data {:action :exported :subject :iam-transfer
                           :transfer-type type :entity entity :records (count records)}}
                   "Exported IAM configuration")
         (vec (sort-by (id/key) records)))))))

(def ^:private writer
  (j/object-mapper {:pretty true
                    :encode-key-fn (fn [k] (if (keyword? k) (name k) (str k)))}))

(defn export-file
  "Write `type` to `path` as deterministic JSON; a single-record export writes
   the bare record (the shape `import!` expects), else an array."
  ([type path] (export-file type path nil))
  ([type path args]
   (let [records (export type args)
         payload (if (= 1 (count records)) (first records) records)]
     (io/make-parents path)
     (spit path (str (j/write-value-as-string payload writer) "\n"))
     (log/info {:id ::export-written
                :data {:action :written :subject :iam-transfer
                       :transfer-type type :path (str path) :records (count records)}}
               "Wrote IAM export")
     path)))

;; ============================================================================
;; Import
;; ============================================================================

(defn records-of [payload] (if (sequential? payload) payload [payload]))

(defn read-payload
  "Accepts a filesystem path/File or a classpath resource name, filesystem
   checked first."
  [path]
  (let [f   (io/file path)
        src (or (when (.exists f) f)
                (when (string? path) (io/resource path)))]
    (when-not src
      (throw (ex-info (str "IAM transfer file not found: " path) {:path (str path)})))
    (read-str (slurp src))))

(defn ref-ids
  "Every id a record points AT, as `relation-key -> #{id}`; these must already
   exist."
  [type record]
  (let [{:keys [refs via]} (spec type)]
    (into {}
          (keep (fn [[rel-key rel-spec]]
                  (when (= :ref rel-spec)
                    (when-let [ids (seq (keep (id/key) (get record rel-key)))]
                      [rel-key (set ids)]))))
          (apply merge refs (vals via)))))

(defn ref-relations
  "Relation-key -> relation for every ref a `type` record may carry, via-refs
   resolved on the via target entity."
  [type]
  (let [{:keys [entity via]} (spec type)
        rels (entity-relations (id/entity entity))]
    (apply merge rels
           (for [[via-key via-refs] via
                 :let [target-rels (entity-relations (get-in rels [via-key :to]))]]
             (select-keys target-rels (keys via-refs))))))

(defn validate-records
  "Dry run over already-parsed records; catches a payload granting access to an
   id no deployed model has (the write path would otherwise mint a silent stub
   row). Writes nothing."
  [type payload]
  (let [records (records-of payload)
        rels    (ref-relations type)
        missing (access/with-principal nil
                  (reduce
                   (fn [acc record]
                     (reduce-kv
                      (fn [acc rel-key ids]
                        ;; `:to` IS the target entity id — do not id/extract it
                        (if-let [target (get-in rels [rel-key :to])]
                          (let [found (into #{}
                                            (map (id/key))
                                            (dataset/search-entity
                                             target
                                             {(id/key) {:_in (vec ids)}}
                                             {(id/key) nil}))
                                gone  (set/difference ids found)]
                            (cond-> acc (seq gone) (update rel-key (fnil into #{}) gone)))
                          acc))
                      acc
                      (ref-ids type record)))
                   {}
                   records))]
    {:records (count records) :missing missing}))

(defn validate
  "`validate-records` for a file path or classpath resource."
  [type path]
  (validate-records type (read-payload path)))

(defn import-via!
  "Write each record's via-refs onto its via target (an app's roles/groups onto
   its SERVICE user), creating the link first."
  [type records write!]
  (access/with-principal nil
    (doseq [record records
            :let [refs (into {}
                             (keep (fn [k] (when-let [v (or (get record k) (get record (name k)))]
                                             [k (mapv #(hash-map (id/key) (or (get % (id/key)) (get % (name (id/key))))) v)])))
                             (mapcat keys (vals (:via (spec type)))))]
            :when (seq refs)]
      (let [app-key (or (get record (id/key)) (get record (name (id/key)))
                        (id/extract (dataset/get-entity :iam/app {:id (or (:id record) (get record "id"))} {(id/key) nil})))
            user (service-user/sync-service-user app-key)]
        (if user
          (write! :iam/user (assoc refs (id/key) (id/extract user)))
          (log/warn {:id ::via-target-missing
                     :data {:action :importing :subject :iam-transfer :transfer-type type
                            :record (or (:id record) (get record "id"))}}
                    "No service user to carry roles/groups (public client?) — skipped"))))))

(defn import-records!
  "Write already-parsed records; `:mode :sync` (default) matches each relation
   the payload MENTIONS exactly (dropping a grant drops it), `:stack` is
   additive-only. `:validate?` defaults true."
  [type payload & {:keys [mode validate? source] :or {mode :sync validate? true}}]
  (let [{:keys [entity]} (spec type)]
    (when validate?
      (let [bad (into #{}
                      (comp (mapcat #(get % :scopes (get % "scopes")))
                            (keep #(or (:name %) (get % "name")))
                            (remove gen/valid-scope-name?))
                      (records-of payload))]
        (when (seq bad)
          (throw (ex-info (str gen/scope-name-message
                               " Rejected: " (str/join ", " (sort bad)))
                          {:type type :invalid-scope-names bad}))))
      (let [{:keys [missing]} (validate-records type payload)]
        (when (seq missing)
          (throw (ex-info (str "IAM transfer payload references ids that do not exist"
                               (when source (str ": " source)))
                          (cond-> {:type type :missing missing}
                            source (assoc :source (str source))))))))
    (log/info {:id ::importing
               :data {:action :importing :subject :iam-transfer
                      :transfer-type type :entity entity :mode mode
                      :source (some-> source str)
                      :records (count (records-of payload))}}
              "Importing IAM configuration")
    (when-not (#{:sync :stack} mode)
      (throw (ex-info (str "Unknown import mode: " mode)
                      {:mode mode :known [:sync :stack]})))
    (let [write! (if (= mode :sync) dataset/sync-entity dataset/stack-entity)
          via-keys (mapcat keys (vals (:via (spec type))))
          strip #(apply dissoc % (concat via-keys (map name via-keys)))
          result (write! entity (if (sequential? payload) (mapv strip payload) (strip payload)))]
      (when (seq via-keys)
        (import-via! type (records-of payload) write!))
      result)))

(defn import!
  "`import-records!` for a file path or classpath resource."
  [type path & {:as opts}]
  (apply import-records! type (read-payload path)
         (mapcat identity (assoc opts :source (str path)))))

;; ============================================================================
;; Per-type sugar
;; ============================================================================
;; :stack + no validation, matching legacy synthigy.iam.util semantics —
;; bootstrap
;; re-runs against live DBs and shipped files may carry legacy ids.

(defn import-role [path] (import! :role path :mode :stack :validate? false))
;; validated, unlike the others — an unknown :datasets id would mint a stub Dataset row
(defn import-group [path] (import! :group path :mode :stack))
(defn import-api  [path] (import! :api  path :mode :stack :validate? false))
(defn import-app  [path] (import! :app  path :mode :stack :validate? false))

(defn export-role  ([] (export :role))  ([args] (export :role args)))
(defn export-group ([] (export :group)) ([args] (export :group args)))
(defn export-user  ([] (export :user))  ([args] (export :user args)))
(defn export-app   ([] (export :app))   ([args] (export :app args)))
(defn export-api   ([] (export :api))   ([args] (export :api args)))

(comment
  (selection :role)
  (export :api {:name {:_eq "Synthigy"}})
  (export-file :role "/tmp/role_iam_admin.json" {:name {:_eq "IAM Admin"}})
  (validate :role "exports/role_iam_admin.json"))
