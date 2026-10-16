;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.engine
  "The operation engine — data in, data out, no transport. Every /data verb
   executes here; synthigy.server.data (HTTP) and synthigy.embedded (in-process)
   are both just callers."
  (:require
   [clojure.core.cache :as cache]
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.traffic :as traffic]
   [synthigy.log.config :as log.config]
   [synthigy.log.query :as log.query]
   [synthigy.db :as db]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.codegen :as codegen]
   [synthigy.dataset.core :as dataset.core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.runtime :as runtime]
   [synthigy.dataset.system :as system]
   [synthigy.dataset.meta :as dataset-meta]
   [synthigy.dataset.version :as dataset-version]
   [synthigy.transit :as transit]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.dataset.sql.rls :as rls]
   [synthigy.dataset.sql.template :as template]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.xsql.api :as xsql]
   [synthigy.xsql.program :as xsql-program]))

(defn normalize-op
  "Normalize an operation name to kebab-case; nil passes through."
  [op]
  (when (string? op)
    (-> op
        (str/replace #"([a-z])([A-Z])" "$1-$2")
        str/lower-case
        (str/replace #"[_\s]+" "-"))))

(def ^:private known-args-keys
  "Operators and modifiers valid at the top of an `args` map."
  #{:_where :_or :_and :_not :_maybe :_count :_agg
    :_limit :_offset :_order_by :_distinct :_join})

(defn coerce-args
  "Coerce JSON-boundary arg quirks before args reach the query layer — typo'd
   _-keys and _order_by shape."
  [args]
  (when (map? args)
    (doseq [[k _] args]
      (when (and (keyword? k)
                 (.startsWith (name k) "_")
                 (not (known-args-keys k)))
        (throw (ex-info
                 (str "Unknown args modifier " (pr-str k)
                      ". Supported: "
                      (str/join " " (sort (map name known-args-keys))))
                 {:code "BAD_ARGS_SHAPE"
                  :rule "args_modifier"
                  :modifier (name k)
                  :supported (mapv name (sort known-args-keys))})))))
  (if-let [ob (:_order_by args)]
    (cond
      (map? ob)
      args

      (sequential? ob)
      (let [bad (some (fn [item]
                        (when-not (and (sequential? item)
                                       (= 2 (count item))
                                       (let [d (second item)
                                             d-name (cond
                                                      (keyword? d) (name d)
                                                      (string? d)  d
                                                      :else        nil)]
                                         (contains? #{"asc" "desc"} d-name)))
                          item))
                      ob)]
        (if bad
          (throw (ex-info
                  (str "_order_by entries must be [field direction] pairs "
                       "where direction is \"asc\" or \"desc\". Got: "
                       (pr-str bad))
                  {:code "BAD_ORDER_BY_SHAPE"
                   :rule "order_by_entry"
                   :argument "_order_by"
                   :hint (str "Use a vector of pairs, e.g. "
                              "[[\"name\" \"asc\"]], not [\"name\" \"asc\"].")}))
          (assoc args :_order_by
                 (apply array-map
                        (mapcat (fn [[field dir]]
                                  [(if (keyword? field) field (keyword field))
                                   (if (keyword? dir) dir (keyword dir))])
                                ob)))))

      :else
      (throw (ex-info
              (str "_order_by must be a vector of [field direction] pairs. "
                   "Got: " (pr-str ob))
              {:code "BAD_ORDER_BY_SHAPE"
               :rule "order_by_shape"
               :argument "_order_by"
               :hint "Use [[\"name\" \"asc\"], …] (vector of pairs)."})))
    args))

(def ^:private write-ops #{"sync" "stack" "delete" "purge" "slice"})

(defn write-op? [{:keys [op]}] (contains? write-ops op))

(def ^:private ops-requiring-entity
  "Op names for which `:entity` is mandatory."
  (into write-ops #{"search" "get" "search-tree" "get-tree"}))

(defn find-fk-candidates
  "Relations attempted in op[:data], as [{:attribute label :target_entity name}
   ...] sorted by attribute, or nil."
  [op model]
  (let [entity-name (:entity op)
        entity      (when entity-name
                      (some (fn [e] (when (= entity-name (:name e)) e))
                            (vals (:entities model))))]
    (when entity
      (let [relations (try (dataset.core/focus-entity-relations model entity)
                           (catch Throwable _ nil))
            rel-by-key (reduce (fn [acc r]
                                 (let [label (:to-label r)]
                                   (if (seq label)
                                     (assoc acc (dk/normalize-key (keyword label)) r)
                                     acc)))
                               {} relations)
            data       (:data op)
            records    (cond (sequential? data) data
                             (map? data)        [data]
                             :else              nil)
            attempted  (into #{}
                             (mapcat (fn [r]
                                       (when (map? r)
                                         (->> (keys r)
                                              (map dk/normalize-key)
                                              (filter rel-by-key))))
                                     records))]
        (when (seq attempted)
          (mapv (fn [k]
                  (let [r (rel-by-key k)]
                    {:attribute     (name k)
                     :target_entity (:name (:to r))}))
                (sort attempted)))))))

(defn enrich-fk-details
  "Replace bare :rule \"reference\" details with model context recovered from
   the operation data."
  [details op]
  (let [model      (try (dataset/deployed-model) (catch Throwable _ nil))
        candidates (when (and model (write-op? op)) (find-fk-candidates op model))
        entity     (:entity op)
        base       (cond-> details entity (assoc :entity entity))]
    (cond
      (nil? (seq candidates)) base
      (= 1 (count candidates))
      (let [{:keys [attribute target_entity]} (first candidates)]
        (cond-> base
          attribute     (assoc :attributes [attribute])
          target_entity (assoc :target_entity target_entity)))
      :else
      (assoc base :candidates candidates))))

(defn enrich-error-details
  "Layer operation/model context onto a translated error; only FK_VIOLATION
   needs it today."
  [translated op]
  (let [data    (ex-data translated)
        details (or (:details data) {})]
    (case (:code data)
      "FK_VIOLATION" (enrich-fk-details details op)
      details)))

(def ^:private error-passthrough-keys
  "Keys lifted from ex-data onto the wire `:error` — the same set the audit row captures."
  [:diagnostics :hint :available :path :entity :relation :operator
   :line :col :start :end
   :rule :attribute :modifier :supported :argument :op
   :args :limit :matched :xids
   :details :retryable])

(defn error-passthrough
  "Lift error-passthrough-keys off an ex-data map, skipping nil/false and empty
   collections."
  [data]
  (reduce (fn [m k]
            (let [v (get data k)]
              (if (and v (or (not (coll? v)) (seq v)))
                (assoc m k (if (= :operator k) (str v) v))
                m)))
          {}
          error-passthrough-keys))

(defn sql-cause
  [^Throwable e]
  (loop [c (ex-cause e)]
    (cond
      (nil? c) nil
      (instance? java.sql.SQLException c) c
      :else (recur (ex-cause c)))))

(defn translate-sql-cause
  "Translate the first SQLException under an ex-info; nil when absent or unrecognized."
  [e]
  (when-let [c (sql-cause e)]
    (some-> db/*db* (db/translate-db-exception c))))

(def ^:private java-leak-patterns
  "Regexes matching Java/Clojure internal messages kept off the wire."
  [#"class\s+[\w.$]+\s+cannot be cast"
   #"^nth not supported"
   #"^Nested problem"
   #"NullPointerException"
   #"clojure\.lang\.[A-Z]"
   #"java\.lang\.[A-Z]"
   #"\.IFn|\.IPersistent"
   #"Don't know how to (?:create|coerce)"])

(defn java-leaky-message? [msg]
  (and (string? msg)
       (boolean (some #(re-find % msg) java-leak-patterns))))

(defn ^:no-doc safe-error-message
  "Sanitize a Java/Clojure-leaky exception message for the wire; returns [clean
   raw] so the caller can still log the raw message."
  [throwable]
  (let [raw    (ex-message throwable)
        clean? (and raw (not (java-leaky-message? raw)))]
    (if clean?
      [raw nil]
      ["Internal error processing operation. The full exception was logged server-side."
       raw])))

(defn ^:no-doc with-request-id
  "Stamp the current request's correlation id onto an :error map, for
   SDK/console error objects that never see the X-Request-Id header."
  [err]
  (if-let [rid (log/request-id)]
    (assoc err :request-id rid)
    err))

(def platform-audience
  "THE platform audience /data, /schema and /logs require on a bearer token; must match api_synthigy.json's :audience."
  "https://synthigy.com")

(def capability-scopes
  "Every scope this engine enforces — capabilities RBAC cannot express; never entity CRUD."
  #{"dataset:load" "dataset:delete" "dataset:deploy"
    "schema:read" "log:read" "log:configure" "dataset:subscription"})

(defn assert-scope!
  "Assert the current user holds scope or any alternates; throws FORBIDDEN
   naming the first (the one to grant)."
  [scope & alternates]
  (when-not (some access/scope-allowed? (cons scope alternates))
    (throw (ex-info (str "Missing required scope: " scope)
                    {:code "FORBIDDEN" :scope scope}))))

(defn lens-model
  "Deployed model a schema read sees: the full model with schema:read, otherwise the caller's own view."
  []
  (if (access/scope-allowed? "schema:read")
    (dataset/deployed-model)
    (daccess/protect-model (dataset/deployed-model))))

(defn schema-model
  "Runtime model a schema read sees: with schema:read the full model or its
   projection onto `role-names`, otherwise the caller's own view."
  [role-names]
  (if (seq role-names)
    (access/with-principal (access/roles->schema-principal role-names)
      (-> (dataset/deployed-model) daccess/protect-model runtime/build))
    (runtime/build (lens-model))))

;;; ============================================================================
;;; Operation Dispatch
;;; ============================================================================

(def dataset-xid (id/entity :dataset/dataset :xid))
(def version-xid (id/entity :dataset/version :xid))

(defmulti execute-operation :op)

(defmethod execute-operation :default
  [ctx]
  (throw (ex-info (str "Unknown operation: '" (:op ctx) "'")
                  {:code "UNKNOWN_OP" :op (:op ctx)})))

(defn assert-map-selections
  "STRICT wire: :selections on a plain wire op is a map or nil, never an XSQL
   string."
  [ctx]
  (when (string? (:selections ctx))
    (throw (ex-info (str "Wire ops take map :selections — XSQL travels via the "
                         "`xsql` operation: {\"op\": \"xsql\", \"xsql\": \"@search name …\"}")
                    {:code "INVALID_SELECTIONS" :rule "operation_shape"})))
  ctx)

(defn offset->line-col
  "1-based {:line :col} for a 0-based byte offset in a non-XSQL source, or nil —
   guards against a log op's :filter (a map, not a string)."
  [source offset]
  (when (and (string? source) (integer? offset) (<= 0 offset (count source)))
    (xsql/line-col source offset)))

(defn enrich-position-error
  "Add :line/:col (and :start/:end for ranges) to error-data when a byte offset
   and source string are both available."
  [error-data op]
  (let [source (or (:template op) (:filter op))]
    (cond-> error-data
      ;; single-point position (template parser)
      (and (:position error-data) source)
      (merge (when-let [lc (offset->line-col source (:position error-data))]
               {:line (:line lc) :col (:col lc)}))
      ;; range form (xsql linter)
      (and (:from error-data) source)
      (assoc :start (offset->line-col source (:from error-data)))
      (and (:to error-data) source)
      (assoc :end (offset->line-col source (:to error-data))))))

(defonce ^:private xsql-doc-cache
  (atom (cache/lru-cache-factory {} :threshold 512)))

(defonce ^:private xsql-doc-cache-stats (atom {:hits 0 :misses 0}))

(defn clear-xsql-doc-cache!
  "Drop all cached XSQL documents (tests/REPL hygiene, never required for
   correctness)."
  []
  (reset! xsql-doc-cache (cache/lru-cache-factory {} :threshold 512))
  (reset! xsql-doc-cache-stats {:hits 0 :misses 0}))

(defn cached-xsql-program
  "document -> {:decls [...]} | {:errors [...]} through the bounded LRU; only
   :error-severity diagnostics gate the request."
  [doc]
  (let [hit? (cache/has? @xsql-doc-cache doc)
        _ (swap! xsql-doc-cache-stats update (if hit? :hits :misses) inc)
        t0 (System/nanoTime)
        c (swap! xsql-doc-cache cache/through-cache doc
                 (fn [_]
                   (let [errors (filterv #(= :error (:severity %))
                                         (xsql-program/lint doc nil))]
                     (if (seq errors)
                       {:errors errors}
                       {:decls (xsql-program/parse-compiled doc)}))))]
    (log/debug {:id ::xsql-compiled
                :data {:cache-hit? hit?
                       :elapsed-us (quot (- (System/nanoTime) t0) 1000)
                       :doc-bytes (count doc)}}
               "XSQL document resolved through cache")
    (cache/lookup c doc)))

(defn coerce-xsql-op
  "Parse an xsql op's :xsql document and replace the synthetic :op \"xsql\" with
   the real @verb so the normal pipeline runs unchanged."
  ([ctx] (coerce-xsql-op ctx nil))
  ([{:keys [xsql params] :as ctx} {:keys [quiet?]}]
  (when-not (string? xsql)
    (throw (ex-info "`xsql` operation requires an :xsql document string"
                    {:code "BAD_OP" :rule "operation_shape"})))
  (doseq [k [:entity :selections :args]]
    (when (some? (get ctx k))
      (throw (ex-info (str "`xsql` operation derives " (name k)
                           " from the document — remove the explicit field")
                      {:code "BAD_OP" :rule "operation_shape" :field k}))))
  (let [{:keys [decls errors]} (cached-xsql-program xsql)
        _ (when (seq errors)
            (let [{:keys [message from to]} (first errors)]
              ;; warn here, not at the cache-miss site — cached failures must still log
              (when-not quiet?
                (log/warn {:id ::xsql-compile-failed
                           :data {:diagnostic-count (count errors)
                                  :message message :from from :to to
                                  :doc-bytes (count xsql)}}
                          "XSQL document failed to compile"))
              (throw (ex-info (str "XSQL parse error: " message)
                              {:code "XSQL_PARSE_ERROR" :diagnostics (vec errors)
                               :from from :to to}))))
        compiled (first (xsql-program/compile-parsed decls params))]
    (when (nil? compiled)
      (throw (ex-info "Empty XSQL operation document"
                      {:code "XSQL_PARSE_ERROR" :rule "operation_shape"})))
    (let [base (-> ctx (dissoc :xsql) (assoc :op (:op compiled)))]
      (cond
        (:mutate compiled)
        (let [dv (:data-var compiled)]
          (assoc base :entity (:entity compiled)
                      :data   (or (get params (keyword dv)) (get params dv))))

        (= "sql-template" (:op compiled))
        (assoc base :template (:sql compiled))

        :else
        (cond-> (assoc base :entity     (:entity compiled)
                            :selections (:selections compiled)
                            :args       (:args compiled))
          (:on compiled)   (assoc :on (:on compiled))
          (:root compiled) (assoc :root (:root compiled))))))))

(defmethod execute-operation "search"
  [ctx]
  (let [{:keys [entity-id args] selection :selections :as ctx} (assert-map-selections ctx)]
    (assoc ctx :result (or (dataset/search-entity entity-id args selection) []))))

(defmethod execute-operation "get"
  [ctx]
  (let [{:keys [entity-id args] selection :selections :as ctx} (assert-map-selections ctx)]
    (when-let [bad (first (filter (comp map? val) args))]
      (throw (ex-info (str "`get` matches one row by unique constraint, so its args are "
                           "plain scalars (e.g. {\"name\": \"alice\"}) — " (pr-str (key bad))
                           " has a map value. Use `search` for predicates.")
                      {:code "BAD_ARGS_SHAPE"
                       :rule "get_args"
                       :argument (name (key bad))})))
    (assoc ctx :result (dataset/get-entity entity-id args selection))))

(defmethod execute-operation "search-tree"
  [{:keys [entity-id on args] selection :selections :as ctx}]
  (when-not on
    (throw (ex-info "search-tree requires :on (relation name)"
                    {:code "MISSING_ON"})))
  (assoc ctx :result
         (or (dataset/search-entity-tree entity-id (keyword on) args selection) [])))

(defmethod execute-operation "get-tree"
  [{:keys [entity-id on root] selection :selections :as ctx}]
  (when-not on
    (throw (ex-info "get-tree requires :on (relation name)"
                    {:code "MISSING_ON"})))
  (when-not root
    (throw (ex-info "get-tree requires :root (starting entity id)"
                    {:code "MISSING_ROOT"})))
  (assoc ctx :result
         (dataset/get-entity-tree entity-id root (keyword on) selection)))

(defmulti mutate
  "Extensible write hook chain, dispatching on [op entity xid priority]."
  (fn [{:keys [op entity-id]} priority]
    [op (id/xid-of entity-id) priority]))

(defn write-result
  "Shapes a sync/stack result per the op's `returning` flag."
  [returning? result]
  (if returning?
    result
    (with-meta {:count (cond (sequential? result) (count result)
                              (nil? result) 0
                              :else 1)}
      {::count-only? true})))

(defmethod mutate :default
  [{:keys [op entity-id args data returning] selection :selections :as ctx} _]
  (assoc ctx :result
         (case op
           "sync"      (write-result returning (dataset/sync-entity entity-id data))
           "stack"     (write-result returning (dataset/stack-entity entity-id data))
           "slice"     (dataset/slice-entity entity-id args selection)
           "delete"    (dataset/delete-entity entity-id data)
           "purge"     (dataset/purge-entity entity-id args selection)
           (throw (ex-info (str "Unknown operation: " op)
                           {:code "UNKNOWN_OP" :op op})))))

(defn execute-operation-chain
  [{:keys [entity-id] :as ctx} operation]
  (let [{:keys [pre post]} (reduce-kv
                            (fn [result k _]
                              (if (keyword? k)
                                result
                                (let [[op eid priority] k]
                                  (if (and (= op operation) (= (id/xid-of entity-id) eid))
                                    (if (pos? priority)
                                      (update result :post (fnil conj []) priority)
                                      (update result :pre (fnil conj []) priority))
                                    result))))
                            nil
                            (methods mutate))
        steps (concat (sort pre) [##Inf] (sort post))]
    (reduce
     (fn [ctx priority]
       (mutate ctx priority))
     ctx
     steps)))

(def system-dataset-problem
  {:code "SYSTEM_DATASET_ENGINE_OWNED"
   :rule "system_dataset"
   :message "Synthigy and OAuth datasets change only through engine upgrades."})

(defn system-change-problem
  [changes]
  {:code "SYSTEM_ENTITY_ALTERED"
   :rule "system_entity"
   :message (str "Deploy changes system entities: "
                 (str/join "; " (map system/change-text (take 10 changes)))
                 (when (> (count changes) 10) (str "; and " (- (count changes) 10) " more")))
   :hint "Accept the global model for these entities (Pull or Revert in the deploy drawer), then deploy."
   :details changes})

(defn problem-ex [{:keys [message] :as problem}]
  (ex-info message (dissoc problem :message)))

(defn assert-removal-allowed!
  "Refuse a non-superuser recall or destroy that touches a system dataset or changes a system entity."
  [dataset-ids version-ids]
  (when-not (access/superuser?)
    (let [system-ids (dataset/system-ids (id/key))]
      (when (some (:datasets system-ids) dataset-ids)
        (throw (problem-ex system-dataset-problem)))
      (let [changes (system/system-changes (dataset/deployed-model)
                                           (dataset.core/preview-recall db/*db* version-ids)
                                           system-ids)]
        (when (seq changes)
          (throw (problem-ex (system-change-problem changes))))))))

(defn removed-dataset
  "Id and deployed version ids of the dataset a destroy names by id or name."
  [ref]
  (when (or (id/extract ref) (:name ref))
    (dataset/get-entity :dataset/dataset
                        (if-let [i (id/extract ref)] {(id/key) i} {:name (:name ref)})
                        {(id/key) nil
                         :versions [{:selections {(id/key) nil}
                                     :args {:deployed {:_eq true}}}]})))

;; Delete Dataset → destroy! (scope check + short-circuit)
(defmethod mutate ["delete" dataset-xid -1]
  [ctx _]
  (assert-scope! "dataset:delete")
  (dataset/model-change
   :destroy (:name (:data ctx)) nil
   (fn []
     (let [found (removed-dataset (:data ctx))]
       (assert-removal-allowed! [(id/extract found)] (map id/extract (:versions found))))
     (log/info {:id ::destroying-dataset
                :data {:action :destroying :subject :dataset
                       :dataset (:data ctx)}}
               "Destroying dataset")
     (reduced (assoc ctx :result (dataset/destroy! (:data ctx)))))))

;; Delete Dataset Version → recall! (scope check + short-circuit)
(defmethod mutate ["delete" version-xid -1]
  [ctx _]
  (assert-scope! "dataset:delete")
  (let [stored (dataset/stored-version (:data ctx))]
    (dataset/model-change
     :recall nil (:name stored)
     (fn []
       (assert-removal-allowed! [(id/extract (:dataset stored))] [(id/extract (:data ctx))])
       (log/info {:id ::recalling-dataset-version
                  :data {:action :recalling :subject :dataset
                         :version (:data ctx)}}
                 "Recalling dataset version")
       (reduced (assoc ctx :result (dataset/recall! (:data ctx))))))))

;; Purge Dataset — forbidden
(defmethod mutate ["purge" dataset-xid -1]
  [_ _]
  (throw (ex-info "Purge not allowed on Dataset. Use delete instead."
                  {:code "FORBIDDEN_OP"})))

;; Purge Dataset Version — forbidden
(defmethod mutate ["purge" version-xid -1]
  [_ _]
  (throw (ex-info "Purge not allowed on Dataset Version. Use delete instead."
                  {:code "FORBIDDEN_OP"})))

(defmethod execute-operation "sync"
  [ctx]
  (execute-operation-chain ctx "sync"))

(defmethod execute-operation "stack"
  [ctx]
  (execute-operation-chain ctx "stack"))

(defmethod execute-operation "slice"
  [ctx]
  (let [ctx (assert-map-selections ctx)]
    (dataset-meta/assert-slice! (:entity-id ctx) (:selections ctx))
    (execute-operation-chain ctx "slice")))

(defmethod execute-operation "purge"
  [ctx]
  (let [ctx (assert-map-selections ctx)]
    (dataset-meta/assert-purge! (:entity-id ctx) (:selections ctx))
    (execute-operation-chain ctx "purge")))

(defmethod execute-operation "delete"
  [ctx]
  (execute-operation-chain ctx "delete"))

;; deliberately not scope-gated — entity/relation RBAC and RLS still apply
(defmethod execute-operation "sql-template"
  [{:keys [template params cached] :as ctx}]
  (assoc ctx :result (template/execute-template template params {:cached cached})))

(defn coerce-log-where-value
  "Coerce a single :where value from JSON shape to the validate-filter-map
   contract."
  [field v]
  (cond
    (vector? v)
    (let [[op-raw & args] v
          op (keyword op-raw)]
      (cond
        (contains? #{:in :not-in} op)
        [op (set args)]

        (= op :matches)
        [op (re-pattern (str (first args)))]

        :else
        (into [op] args)))

    (and (= field :level) (string? v))
    (keyword v)

    :else v))

(defn coerce-log-where
  "Walk :where, expanding nested {:data {...}} / {:ctx {...}} into path-keyed
   entries; two levels only."
  [where]
  (reduce-kv
   (fn [acc k v]
     (cond
       (and (contains? #{:data :ctx} k) (map? v))
       (reduce-kv (fn [a k2 v2]
                    (assoc a [k k2] (coerce-log-where-value [k k2] v2)))
                  acc v)

       :else
       (assoc acc k (coerce-log-where-value k v))))
   {}
   where))

(def ^:private log-filter-key-aliases
  "Snake-case wire keys → kebab-case keys for the filter top level."
  {:order_by :order-by
   :group_by :group-by
   :bucket_ms :bucket-ms})

(defn coerce-log-filter
  "Lift the wire filter map into the shape validate-filter-map accepts."
  [filter]
  (let [renamed (reduce-kv (fn [acc k v]
                             (assoc acc (get log-filter-key-aliases k k) v))
                           {} filter)]
    (cond-> renamed
      (:where renamed)    (update :where coerce-log-where)
      (:order-by renamed) (update :order-by
                                  (fn [[f d]]
                                    [(keyword f) (keyword d)])))))

(defmethod execute-operation "log-query"
  [{:keys [filter] :as ctx}]
  (assert-scope! "log:read")
  (let [coerced (coerce-log-filter (or filter {}))]
    (try
      (log.query/validate-filter-map coerced)
      (catch clojure.lang.ExceptionInfo e
        (throw (ex-info (ex-message e)
                        (assoc (ex-data e) :code "INVALID_LOG_FILTER")))))
    (assoc ctx :result (log.query/query coerced))))

(defmethod execute-operation "log-histogram"
  [{:keys [filter] :as ctx}]
  (assert-scope! "log:read")
  (let [coerced (coerce-log-filter (or filter {}))]
    (when-not (:bucket-ms coerced)
      (throw (ex-info "`log-histogram` requires bucket_ms"
                      {:code "INVALID_LOG_FILTER"})))
    (try
      (log.query/validate-filter-map coerced)
      (catch clojure.lang.ExceptionInfo e
        (throw (ex-info (ex-message e)
                        (assoc (ex-data e) :code "INVALID_LOG_FILTER")))))
    (assoc ctx :result {:bucket-ms (:bucket-ms coerced)
                        :buckets   (log.query/query coerced)})))

(defmethod execute-operation "traffic-stats"
  [{:keys [since until] :as ctx}]
  (assert-scope! "log:read")
  (doseq [[k v] [[:since since] [:until until]]]
    (when (and (some? v) (not (string? v)))
      (throw (ex-info (str "`traffic-stats` " (name k) " must be a string")
                      {:code "INVALID_LOG_FILTER" :field k}))))
  (assoc ctx :result (traffic/report {:since since :until until})))

(defmethod execute-operation "log-config"
  [{:keys [action config] :as ctx}]
  (assert-scope! "log:configure")
  (let [action (or (some-> action name keyword) :get)]
    (case action
      :get   (assoc ctx :result {:config   (log.config/get-config)
                                 :routing  (log/routing-snapshot)})
      :set   (do
               (when-let [err (log.config/validate-config (or config {}))]
                 (throw (ex-info err {:code "INVALID_LOG_CONFIG"})))
               (log.config/set-config! (or config {}))
               (assoc ctx :result {:config  (log.config/get-config)
                                   :routing (log/routing-snapshot)}))
      :clear (do (log.config/clear-config!)
                 (assoc ctx :result {:config nil
                                     :routing (log/routing-snapshot)}))
      (throw (ex-info (str "Unknown log-config action: " action)
                      {:code "INVALID_LOG_CONFIG_ACTION"})))))

(defmethod execute-operation "deployed-model"
  [ctx]
  (assert-scope! "dataset:load")
  (assoc ctx :result (transit/->transit (lens-model))))

(defmethod execute-operation "runtime-model"
  [ctx]
  (assert-scope! "dataset:load")
  (assoc ctx :result (transit/->transit (runtime/build (lens-model)))))

(defmethod execute-operation "synthigy-datasets"
  [ctx]
  (assert-scope! "dataset:load")
  (assoc ctx :result (vec (sort (:datasets (dataset/system-ids (id/key)))))))

(defmethod execute-operation "schema"
  [{:keys [entities roles] :as ctx}]
  (assoc ctx :result (daccess/schema (schema-model roles) entities)))

;; Who this request runs as, after acting_as; :client is the HTTP handler's delay.
(defmethod execute-operation "whoami"
  [{:keys [principal client] :as ctx}]
  (let [names #(->> (vals %) (keep :name) sort vec)]
    (assoc ctx :result {:xid (some-> principal id/extract)
                        :name (:name principal)
                        :active (not (false? (:active principal)))
                        :superuser (access/superuser?)
                        :roles (names (:roles principal))
                        :groups (names (:groups principal))
                        :client (force client)})))

;; Codegen IR — XSQL program source → typed, language-neutral operation IR.
(defn describe-files
  "`describe`'s input as `[{:path :source}]`: `sources` (one per .xsql file) or a single `source`."
  [{:keys [source sources]}]
  (cond
    (and (sequential? sources) (seq sources)
         (every? #(string? (or (:source %) (get % "source"))) sources))
    (mapv (fn [f] {:path (or (:path f) (get f "path")) :source (or (:source f) (get f "source"))})
          sources)

    (string? source) [{:source source}]

    :else
    (throw (ex-info "`describe` requires :sources [{path, source}] (one per .xsql file) or a single :source string"
                    {:code "BAD_OP" :rule "operation_shape"}))))

(defmethod execute-operation "describe"
  [{:keys [params roles] :as ctx}]
  (let [files  (describe-files ctx)
        schema (daccess/schema (schema-model roles))]
    (assoc ctx :result (codegen/describe schema files params))))

(defn deploy-payload
  "Normalize what a client sent (export string, version map, or either with an export-string `:model`) into a dataset version map."
  [data]
  (let [data (if (string? data) (transit/<-transit data) data)]
    (if (string? (:model data))
      (update data :model transit/<-transit)
      data)))

(defn deploy-version
  "The version a deploy writes: ids, name, configuration, model and a dataset reference with an id, nothing nested."
  [data]
  (let [version (deploy-payload data)
        dataset (select-keys (:dataset version) [:xid :euuid :name])]
    (when-not (id/extract dataset)
      (throw (ex-info "Deploy needs :dataset with an id."
                      {:code "BAD_OP" :rule "operation_shape" :argument "dataset"})))
    (assoc (select-keys version [:xid :euuid :name :configuration :model])
           :dataset dataset)))

(defn guard-problem
  "RLS_GUARD_BROKEN when `candidate` breaks an RLS guard it authors in `preview`, else nil."
  [preview candidate]
  (try
    (rls/assert-guards-compile! preview candidate)
    nil
    (catch clojure.lang.ExceptionInfo e
      (when (= "RLS_GUARD_BROKEN" (:code (ex-data e)))
        {:code "RLS_GUARD_BROKEN"
         :rule "rls_guard"
         :message (ex-message e)
         :details (mapv #(select-keys % [:entity :guard-id :operation :unresolved]) (:guards (ex-data e)))}))))

(defn deploy-problems
  "Errors and warnings deploying `version` raises, each shaped as a wire error; system changes only warn a superuser."
  [version]
  (let [superuser (access/superuser?)
        stored (dataset/stored-version version)
        system-ids (dataset/system-ids (id/key))
        global (or (dataset/deployed-model)
                   (dataset.core/map->ERDModel {:entities {} :relations {}}))
        candidate (dataset/persisted-model (:model version))
        preview (dataset.core/preview-model db/*db* (assoc version :model candidate))
        name-conflicts (system/new-name-conflicts global preview)
        conflicts (mapv system/conflict-details (system/new-field-conflicts global preview))
        changes (system/system-changes global preview system-ids)
        rls-additions (when-not (contains? (:datasets system-ids) (id/extract (:dataset version)))
                        (system/system-rls-additions global candidate system-ids))
        broken-guards (guard-problem preview candidate)
        name-taken (dataset-version/name-problem version)]
    (cond-> {:errors [] :warnings []}
      name-taken
      (update :errors conj name-taken)

      broken-guards
      (update :errors conj broken-guards)

      (seq rls-additions)
      (update :errors conj {:code "SYSTEM_ENTITY_RLS"
                            :rule "system_entity_rls"
                            :message (str "RLS on system entities comes only from the Synthigy and OAuth datasets: "
                                          (str/join ", " (map system/rls-addition-text rls-additions)) ".")
                            :hint "Remove the RLS from these entities in this dataset; put guards on your own entities instead."
                            :details rls-additions})

      (dataset/deployed-version-altered? version stored)
      (update :errors conj {:code "IMMUTABLE_VERSION_ALTERED"
                            :rule "deploy_immutable"
                            :message "Deployed version is immutable; create a new version to change the model."})

      (and (not superuser)
           (some (:datasets system-ids) [(id/extract (:dataset version)) (id/extract (:dataset stored))]))
      (update :errors conj system-dataset-problem)

      (seq name-conflicts)
      (update :errors conj {:code "ENTITY_NAME_CONFLICT"
                            :rule "entity_name"
                            :message (str "Entity names collide: " (str/join "; " (map system/name-conflict-text name-conflicts)))
                            :hint "Rename the entity; each entity name maps to one table."
                            :details name-conflicts})

      (seq conflicts)
      (update :errors conj {:code "FIELD_KEY_CONFLICT"
                            :rule "field_key"
                            :message (str "Field keys collide: " (str/join "; " (map system/conflict-text conflicts)))
                            :hint "Rename the attribute or relation label so each field key is used once per entity."
                            :details conflicts})

      (seq changes)
      (update (if superuser :warnings :errors) conj (system-change-problem changes)))))

(defmethod execute-operation "deploy-check"
  [ctx]
  (assert-scope! "dataset:deploy")
  (assoc ctx :result (deploy-problems (deploy-version (:data ctx)))))

(defmethod execute-operation "deploy"
  [ctx]
  (assert-scope! "dataset:deploy")
  (let [version (deploy-version (:data ctx))]
    (dataset/model-change
     :deploy (get-in version [:dataset :name]) (:name version)
     (fn []
       (let [{:keys [errors warnings]} (deploy-problems version)]
         (when-let [problem (first errors)]
           (throw (problem-ex problem)))
         (doseq [{:keys [code message details]} warnings]
           (log/warn {:id ::deploy-warning
                      :data {:action :deploying :subject :dataset :code code
                             :version (:name version) :dataset (id/extract (:dataset version))
                             :details details}}
                     message))
         (dataset/deploy! version))))
    (assoc ctx :result {:deployed true
                        :version (:name version)
                        :dataset (id/extract (:dataset version))})))

(defn ensure-tree-on-relation
  "Inject the on-relation into :selections if absent, so tree-op consumers can
   rebuild parent->child links; skips XSQL string selections."
  [op]
  (if-let [on (and (#{"search-tree" "get-tree"} (:op op)) (:on op))]
    (let [k   (keyword on)
          sel (:selections op)]
      (cond
        (or (nil? sel) (map? sel))
        (cond-> op
          (not (contains? (or sel {}) k))
          (assoc-in [:selections k] [{:selections nil}]))
        :else op))
    op))

(def ^:private audit-ops
  "Op names that emit `:synthigy.server.data/op-completed` on finish; meta-ops excluded."
  (into write-ops #{"search" "get" "search-tree" "get-tree" "sql-template"}))

(defn selection-shape
  "Compact tree representation of a selections map for info-level logging; nil
   for non-map shapes."
  [sel]
  (when (map? sel)
    (let [{:keys [scalars relations]}
          (reduce-kv
           (fn [acc k v]
             (if (sequential? v)
               (assoc-in acc [:relations k]
                         (selection-shape (some-> v first :selections)))
               (update acc :scalars (fnil inc 0))))
           {}
           sel)]
      (cond-> {}
        scalars         (assoc :scalars scalars)
        (seq relations) (assoc :relations relations)))))

(defn predicate-keys
  "Set of key paths through a filter map to leaf operator values, values
   stripped — a compact summary alongside the full :filter/:args."
  [filter-map]
  (letfn [(walk [path m]
            (cond
              (map? m)        (mapcat (fn [[k v]] (walk (conj path k) v)) m)
              (sequential? m) (mapcat #(walk path %) m)
              :else           [path]))]
    (->> (walk [] filter-map) (filter seq) set)))

(defn write-records-in
  "How many records the write op carried in — vector :data -> count, single map
   -> 1, missing -> nil."
  [op]
  (let [d (:data op)]
    (cond
      (sequential? d) (count d)
      (map? d)        1
      :else           nil)))

(defn write-bytes-in
  "Approximate serialized byte size of the write op's input, via pr-str length
   as a cheap proxy."
  [op]
  (when-let [d (:data op)]
    (try (count (.getBytes ^String (pr-str d) "UTF-8"))
         (catch Throwable _ nil))))

(defn audit-op-completed!
  "Emit :synthigy.server.data/op-completed info row for one /data op; always
   fires for ops in audit-ops."
  [op started-ms result]
  (let [data       (:data result)
        row-count  (cond
                     (sequential? data) (count data)
                     (some? data)       1
                     :else              0)
        elapsed-ms (- (System/currentTimeMillis) started-ms)
        write?     (write-op? op)
        status     (if (:ok result) :ok :error)
        user-xid   (try (:xid access/*principal*)
                        (catch Throwable _ nil))
        err        (when (= status :error) (:error result))
        throwable  (when (= status :error) (::throwable result))
        payload    (cond-> {:action     :finished
                            :subject    :request
                            :audit-kind (if write? :write :read)
                            :op         (:op op)
                            :status     status
                            :elapsed-ms elapsed-ms
                            :row-count  row-count}
                     user-xid                (assoc :user-xid user-xid)
                     (:entity op)            (assoc :entity (:entity op))
                     (:on op)                (assoc :on (:on op))
                     (and (not write?)
                          (:selections op))  (-> (assoc :selection-shape
                                                        (selection-shape (:selections op)))
                                                 (assoc :selections (:selections op)))
                     (and (not write?)
                          (:filter op))      (-> (assoc :predicate-keys
                                                        (predicate-keys (:filter op)))
                                                 (assoc :filter (:filter op)))
                     (and write?
                          (some? (:data op))) (-> (assoc :records-in
                                                         (write-records-in op))
                                                  (assoc :bytes-in
                                                         (write-bytes-in op)))
                     (:args op)              (assoc :args (:args op))
                     (:template op)          (assoc :template (:template op))
                     err                     (assoc :error
                                                    (select-keys
                                                     err
                                                     (into [:code :message]
                                                           error-passthrough-keys))))
        msg        (cond
                     (= status :error)
                     (str (if write? "write" "read") " failed"
                          (when-let [code (:code err)] (str " (" code ")")))
                     :else
                     (if write? "write finished" "read finished"))]
    (traffic/count! :ops)
    (if (= status :error)
      (log/error {:id :synthigy.server.data/op-completed :data payload
                  :error throwable} msg)
      (log/info {:id :synthigy.server.data/op-completed :data payload} msg))))

(defn ^:no-doc maybe-audit-op!
  "Write an op-completed audit row when op's verb is audited. Never throws;
   public so embedded audits identically to the wire."
  [op started-ms result]
  (when (contains? audit-ops (:op op))
    (try (audit-op-completed! op started-ms result)
         (catch Throwable _))))

(defn assert-op-shape!
  "Reject an operation that carries no verb, or a verb that needs an entity and
   has none; returns ctx."
  [ctx]
  (when (nil? (:op ctx))
    (throw (ex-info "Operation missing :op"
                    {:code "BAD_OP"
                     :rule "operation_shape"})))
  (when (and (ops-requiring-entity (:op ctx))
             (nil? (:entity ctx)))
    (throw (ex-info (str "Op '" (:op ctx) "' requires :entity")
                    {:code "MISSING_ENTITY"
                     :rule "operation_arg"
                     :op (:op ctx)
                     :argument "entity"})))
  ctx)

(defn ^:no-doc operation-context
  "Resolve one wire operation into the context execute-operation dispatches on."
  [ctx op]
  (as-> (merge op ctx) ctx
    (if (= "xsql" (:op ctx)) (coerce-xsql-op ctx) ctx)
    (assert-op-shape! ctx)
    (if (= "delete" (:op ctx))
      (update ctx :data
              (fn [data]
                (cond
                  (map? data) data
                  (sequential? data) (sql-query/check-delete-batch! data)
                  :else (throw (ex-info "delete :data must be a map or a vector of xid strings"
                                        {:code "BAD_DATA_SHAPE"
                                         :rule "delete_data"
                                         :op "delete"})))))
      ctx)
    (update ctx :args coerce-args)
    (if (:entity ctx)
      (assoc ctx :entity-id (sql-query/resolve-entity (:entity ctx)))
      ctx)))

(defn execute-operations
  "Execute operations writes-first: writes run sequentially in order, then reads
   in parallel via futures; results return in request order."
  [ctx operations]
  (letfn [(run-op
            [op]
            (try
              (let [ctx (operation-context ctx op)]
                {:data (:result (execute-operation ctx))
                 :ok true
                 ::op-shape (select-keys ctx [:entity :entity-id :selections])})
              (catch clojure.lang.ExceptionInfo e
                (let [raw        (ex-data e)
                      translated (when-not (:code raw) (translate-sql-cause e))
                      data       (enrich-position-error
                                  (merge raw (ex-data translated))
                                  op)
                      err        (merge {:message (if translated
                                                    (str (ex-message e) ": " (ex-message translated))
                                                    (ex-message e))
                                         :code    (or (:code data) "OPERATION_ERROR")}
                                        (error-passthrough data))]
                  {:error err :ok false}))
              (catch java.sql.SQLException e
                (if-let [translated (some-> db/*db* (db/translate-db-exception e))]
                  (let [data    (ex-data translated)
                        code    (:code data)
                        details (enrich-error-details translated op)
                        err     (cond-> {:message (ex-message translated)
                                         :code    code}
                                  (seq details)     (assoc :details details)
                                  (:hint data)      (assoc :hint (:hint data))
                                  (:retryable data) (assoc :retryable true))]
                    (case code
                      "TIMEOUT"
                      (log/warn {:id ::db-statement-timeout :error e}
                                "DB statement timed out")
                      ("DB_ERROR" "DB_UNAVAILABLE")
                      (log/error! {:id ::operation-failed-sql
                                   :msg "Operation failed (database error)"
                                   :data {:op (:op op) :entity (:entity op) :code code}}
                                  e)
                      (log/debug {:id ::db-error-translated
                                  :data {:code code}}
                                 "Translated DB error to wire code"))
                    {:error err :ok false})
                  (let [[clean raw] (safe-error-message e)]
                    (log/error! {:id ::operation-failed-sql
                                 :msg "Operation failed (untranslated SQL exception)"
                                 :data {:op (:op op) :entity (:entity op)
                                        :raw-message raw}}
                                e)
                    {:error {:message clean :code "INTERNAL_ERROR"}
                     ::throwable e
                     :ok false})))
              (catch Throwable e
                (let [[clean raw] (safe-error-message e)]
                  (log/error! {:id ::operation-failed
                               :msg "Operation failed"
                               :data {:op (:op op) :entity (:entity op)
                                      :raw-message raw}}
                              e)
                  {:error {:message clean :code "INTERNAL_ERROR"}
                   ::throwable e
                   :ok false}))))
          (process-operation
            [op]
            (let [started (System/currentTimeMillis)
                  result  (run-op op)]
              (maybe-audit-op! op started result)
              (cond-> result
                (map? (:error result)) (update :error with-request-id))))]
    (let [;; resolve xsql ops to their real @verb first — else an xsql write schedules as a read
          indexed (map-indexed
                   (fn [i op]
                     (let [op (update op :op normalize-op)]
                       [i (if (= "xsql" (:op op))
                            (try (coerce-xsql-op op {:quiet? true})
                                 (catch Throwable _ op))
                            op)]))
                   operations)
          writes  (filterv (fn [[_ op]] (write-op? op)) indexed)
          reads   (filterv (fn [[_ op]] (not (write-op? op))) indexed)
          ;; Execute writes sequentially, preserving order
          write-results (mapv (fn [[i op]] [i (process-operation op)]) writes)
          ;; Execute reads in parallel (future conveys bindings automatically)
          read-results  (if (> (count reads) 1)
                          (->> reads
                               (mapv (fn [[i op]] [i (future (process-operation op))]))
                               (mapv (fn [[i fut]] [i @fut])))
                          (mapv (fn [[i op]] [i (process-operation op)]) reads))]
      ;; Reassemble in original order
      (->> (into write-results read-results)
           (sort-by first)
           (mapv second)))))

(defn lint-source
  "Lint an XSQL source string against the IAM-projected schema; entity optional
   (schema-aware checks only when present), op defaults to search."
  [{:keys [source entity op] :or {op "search"}}]
  (let [source (or source "")
        op     (when op (name op))
        model  (daccess/protect-model (dataset/deployed-model))
        schema (daccess/schema model)
        diags  (cond
                 (str/blank? source)
                 []

                 (and entity (get-in schema [:entities entity]))
                 (xsql/lint source schema entity op)

                 :else
                 (xsql/lint source nil nil op))]
    (mapv (fn [d]
            (assoc d
                   :start (xsql/line-col source (:from d))
                   :end   (xsql/line-col source (:to d))))
          diags)))

(defn compile-operations
  "Compile a /data request body's operations to the wire ops the executor
   receives — `execute`'s coercion, stopped before execution."
  [{:keys [operations]}]
  (when (empty? operations)
    (throw (ex-info "No operations provided" {:code "NO_OPERATIONS"})))
  (mapv (fn [op]
          (let [op (-> op ensure-tree-on-relation (update :op normalize-op))]
            (try
              {:ok true
               :operation (assert-op-shape!
                           (if (= "xsql" (:op op)) (coerce-xsql-op op) op))}
              (catch clojure.lang.ExceptionInfo e
                (let [data (enrich-position-error (ex-data e) op)]
                  {:ok false
                   :error (merge {:message (ex-message e)
                                  :code    (or (:code data) "OPERATION_ERROR")}
                                 (error-passthrough data))})))))
        operations))

(defn resolve-acting-as
  "Resolve the acting_as user; USER_NOT_FOUND if absent, USER_INACTIVE only on
   explicit :active false (nil/missing passes)."
  [acting-as]
  (let [user-ctx (iam.context/get-user-context acting-as)]
    (cond
      (nil? user-ctx)
      (throw (ex-info "User not found"
                      {:code "USER_NOT_FOUND" :acting-as acting-as}))

      (false? (:active user-ctx))
      (throw (ex-info "User is inactive"
                      {:code "USER_INACTIVE" :acting-as acting-as}))

      :else
      {:principal user-ctx})))

(defn strip-eid
  "Recursively remove the internal :_eid (Postgres serial PK) from a result
   value — a storage detail, never the client's business."
  [v]
  (cond
    (map? v)        (reduce-kv (fn [m k val]
                                 (if (= k :_eid) m (assoc m k (strip-eid val))))
                               {} v)
    (sequential? v) (mapv strip-eid v)
    :else           v))

(defn ^:no-doc shape-data
  "Shape one operation's data like the wire does: strip keys not in :selections,
   then apply key-fn if given."
  [key-fn op data]
  (cond
    (::count-only? (meta data))
    data

    (and (:entity op) (coll? data))
    (let [entity-id (or (:entity-id op) (sql-query/resolve-entity (:entity op)))
          sel (when (map? (:selections op))
                (dataset/normalize-selection (:selections op)))
          strip #(dataset/strip-to-selection entity-id sel %)
          key-xf #(dk/transform-result entity-id key-fn % sel)
          xf (cond
               (and sel key-fn) (comp key-xf strip)
               sel strip
               key-fn (comp strip-eid key-xf)
               :else strip-eid)]
      (if (sequential? data) (mapv xf data) (xf data)))

    (and (nil? (:entity op)) key-fn (sequential? data))
    (mapv #(update-keys % key-fn) data)

    :else data))

(defn transform-results
  "Shape each operation's result: strip to :selections and apply key-fn; skips
   failed results and entity-less ops."
  [key-fn operations results]
  (mapv (fn [op result]
          (let [op (merge op (::op-shape result))
                result (dissoc result ::op-shape ::throwable)]
            (if (:ok result)
              (assoc result :data (shape-data key-fn op (:data result)))
              result)))
        operations results))

(defn execute
  "Run a /data (or /logs) request body and return {:results [...]} — the plane
   with no HTTP, auth, parsing, or negotiation around it."
  [{:keys [operations key_format acting_as]} auth]
  (when (empty? operations)
    (throw (ex-info "No operations provided" {:code "NO_OPERATIONS"})))
  (let [key-fn   (dk/format->key-fn (or key_format "snake"))
        user-ctx (if acting_as (resolve-acting-as acting_as) auth)]
    (access/with-principal (:principal user-ctx)
      (let [operations (mapv ensure-tree-on-relation operations)
            results (execute-operations
                     {:principal (:principal user-ctx)
                      ;; claims ride from the TOKEN, never the impersonated user
                      :claims (:claims auth)
                      ;; always present: operation-context merges the op body UNDER this map
                      :client (:client auth)}
                     operations)]
        {:results (transform-results key-fn operations results)}))))

