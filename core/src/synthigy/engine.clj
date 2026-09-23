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

(ns synthigy.engine
  "The operation engine — data in, data out, no transport. Every /data verb
   executes here; synthigy.server.data (HTTP) and synthigy.embedded (in-process)
   are both just callers. No oauth/ring/lifecycle dependency — an in-process
   caller must never need an HTTP stack. See docs/core/synthigy/engine.md."
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
   [synthigy.transit :as transit]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.dataset.sql.template :as template]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.xsql.api :as xsql]
   [synthigy.xsql.program :as xsql-program]))

(defn normalize-op
  "Normalize an operation name
   (camelCase/PascalCase/snake_case/kebab-case/spaces) to kebab-case; nil passes
   through."
  [op]
  (when (string? op)
    (-> op
        (str/replace #"([a-z])([A-Z])" "$1-$2")
        str/lower-case
        (str/replace #"[_\s]+" "-"))))

(def ^:private known-args-keys
  "Operators + modifiers valid at the top of an `args` map. Anything else
   `_`-prefixed at the top level is a typo — caught in `coerce-args`
   before the args reach the SQL builder, where the failure surfaces as
   a Java cast and gets sanitized to INTERNAL_ERROR.

   Plain attribute / relation keys at the top level are NOT gated here —
   the deeper resolver in `sql.query/selection->schema` knows the
   per-entity attribute set and raises `UNKNOWN_ATTRIBUTE` with a hint."
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

      ;; Without this gate a flat ["name" "asc"] silently destructures as
      ;; iterators over the
      ;; characters and crashes deep in the SQL builder as an opaque
      ;; INTERNAL_ERROR.
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
  "Op names for which `:entity` is mandatory. Caught at the op-shape gate
   in `operation-context` before any expensive work runs — failure mode
   `MISSING_ENTITY` rather than the late-binding crash that used to
   surface as `INTERNAL_ERROR`. Ops absent from this set (e.g.
   `sql-template`, `deployed-model`, `schema`, `log-query`) don't need
   an entity ref.

   Derived from `write-ops` (like `audit-ops` below) so a new write op is
   picked up here automatically instead of silently losing its entity gate."
  (into write-ops #{"search" "get" "search-tree" "get-tree"}))

;; Error enrichment: the backend Translator (db/translate-db-exception) maps a
;; native SQL
;; exception to a backend-neutral :code + :details; this layers on operation
;; context + the
;; deployed model that the translator can't see. SQLite FK violations carry no
;; per-attribute
;; context, so find-fk-candidates walks the op's data to recover it.

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
  "Keys lifted from an `ex-info`'s data onto the wire `:error` map — and the
   SAME set the audit row captures, so the two cannot drift. They had:
   `:available` / `:operator` / `:op` reached the client but were silently
   absent from the log, because each site kept its own hand-written list.

   `:message` and `:code` are NOT here — they're built separately (message
   comes from `ex-message`, code carries a default)."
  [:diagnostics :hint :available :path :entity :relation :operator
   :line :col :start :end
   ;; Wire-triage fields added 2026-05-30 so the typed codes
   ;; (UNKNOWN_ATTRIBUTE, BAD_ARGS_SHAPE, MISSING_ENTITY, BAD_OP) surface
   ;; their context to the client instead of being stripped by the envelope.
   :rule :attribute :modifier :supported :argument :op
   :args :limit :matched :xids])

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

(def ^:private java-leak-patterns
  "Regexes that match Java/Clojure internal exception messages we don't
   want surfaced on the wire. Hits the response with a sanitized generic
   message; the raw original goes into the server log only."
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
  "THE platform audience (PLAN-AUDIENCE-BINDING.md REV 3) — what /data,
   /schema, and /logs require on a bearer token. MUST match
   resources/exports/api_synthigy.json's :audience (asserted by
   synthigy.iam.scope-contract-test). Passed to `auth/authenticate-request`
   so a token minted for a DIFFERENT declared audience, or for the
   identity-only default (`synthigy.oauth.core/oidc-audience`), is rejected
   here — a client must explicitly request this audience and be linked via
   `:iam/app`->`:apis` to receive it. See
   synthigy.oauth.authentication/token->user-context's :audience option."
  "https://synthigy.com")

(def capability-scopes
  "Every scope this engine ENFORCES — the complete set passed to
   `assert-scope!`, here and in `synthigy.server.subscription`. These gate
   ENGINE CAPABILITIES that RBAC cannot express: there is no entity to
   grant on for \"may deploy a schema\", \"may read logs\", or \"may hold
   open a model-deploy subscription\". Entity CRUD is RBAC's job and must
   never appear here (see the scope-strategy doctrine); third-party API
   scopes are issued as token claims and enforced by the consumer, never
   here.

   Kept as data so `synthigy.iam.scope-contract-test` can assert every
   enforced scope is actually DECLARED in resources/exports/api_synthigy.json
   and GRANTED by at least one shipped role. Without that test the three
   sets drift silently: on 2026-08-04, four of these six were enforced here
   while granted to no role at all, making the modeler, schema
   introspection, codegen `describe` and the whole log cockpit
   unreachable for every non-superuser."
  #{"dataset:load" "dataset:delete" "dataset:deploy"
    "schema:read" "log:read" "log:configure" "dataset:subscription"})

(defn assert-scope!
  "Assert the current user holds scope or any alternates; throws FORBIDDEN
   naming the first (the one to grant)."
  [scope & alternates]
  (when-not (some access/scope-allowed? (cons scope alternates))
    (throw (ex-info (str "Missing required scope: " scope)
                    {:code "FORBIDDEN" :scope scope}))))

;;; ============================================================================
;;; Operation Dispatch
;;; ============================================================================

;; Entity xids for Dataset and Dataset Version, resolved from the `defentity`
;; registry (see `synthigy.dataset`) rather than hand-copied literals.
;; XID-only: the engine is xid-native, and a euuid-authored model is converted
;; on the frontend at import — so there is no euuid dispatch value to register.
(def dataset-xid (id/entity :dataset/dataset :xid))
(def version-xid (id/entity :dataset/version :xid))

(defmulti execute-operation :op)

(defmethod execute-operation :default
  [ctx]
  (throw (ex-info (str "Unknown operation: '" (:op ctx) "'")
                  {:code "UNKNOWN_OP" :op (:op ctx)})))

;; XSQL transport is STRICT: the only XSQL-aware wire shape is the `xsql` op,
;; whose :xsql
;; field carries a full operation DOCUMENT. No complecting — a plain wire op
;; with a string
;; :selections, or an xsql op with explicit :entity/:selections/:args, is an
;; error.

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

;; XSQL document cache: generated clients send byte-identical documents every
;; call, so
;; parse+lint is pure repeat work. Syntax-only (nil schema) so entries never go
;; stale on
;; model deploys — unlike sql-template's cache, no deploy hook needed. Bounded
;; LRU; lint
;; failures are cached too (a retried broken doc shouldn't re-lint).

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
              ;; Warn here (not at the cache-miss site) so a request against a
              ;; cached failure still logs.
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
        ;; wire :params keys are keywordized by json/read-str; fall back to
        ;; string for transit/edn callers.
        (let [dv (:data-var compiled)]
          (assoc base :entity (:entity compiled)
                      :data   (or (get params (keyword dv)) (get params dv))))

        (= "sql-template" (:op compiled))
        (assoc base :template (:sql compiled))

        :else
        ;; Tree ops carry :on (+ :root for get-tree) as top-level wire keys.
        (cond-> (assoc base :entity     (:entity compiled)
                            :selections (:selections compiled)
                            :args       (:args compiled))
          (:on compiled)   (assoc :on (:on compiled))
          (:root compiled) (assoc :root (:root compiled))))))))

(defmethod execute-operation "search"
  [ctx]
  (let [{:keys [entity-id args] selection :selections :as ctx} (assert-map-selections ctx)]
    ;; Empty match set is [] on the wire, never null.
    (assoc ctx :result (or (dataset/search-entity entity-id args selection) []))))

(defmethod execute-operation "get"
  [ctx]
  (let [{:keys [entity-id args] selection :selections :as ctx} (assert-map-selections ctx)]
    ;; get args must be scalars — an unchecked map value ({:_eq v}) reaches JDBC
    ;; and binds
    ;; as hstore, surfacing "No hstore extension installed" instead of the real
    ;; mistake.
    (when-let [bad (first (filter (comp map? val) args))]
      (throw (ex-info (str "`get` matches one row by unique constraint, so its args are "
                           "plain scalars (e.g. {\"name\": \"alice\"}) — " (pr-str (key bad))
                           " has a map value. Use `search` for predicates.")
                      {:code "BAD_ARGS_SHAPE"
                       :rule "get_args"
                       :argument (name (key bad))})))
    (assoc ctx :result (dataset/get-entity entity-id args selection))))

;; Tree operations: search-tree walks :on UP to ancestors from matched entities;
;; get-tree
;; walks DOWN from :root via reverse FK. Tree aggregation belongs in
;; sql-template, not here.

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
  "Extensible write hook chain, dispatching on [op entity-id priority]: negative
   = pre-hooks, ##Inf = default handler, positive = post-hooks. reduced in a
   pre-hook short-circuits."
  (fn [{:keys [op entity-id]} priority]
    [op entity-id priority]))

(defn- write-result
  "Shapes a sync/stack result per the op's `returning` flag. `returning?`
   false/absent -> a bare {:count n} marker, tagged via metadata so
   shape-data passes it through untouched instead of stripping it as
   entity data. Any future `[\"sync\"/\"stack\" entity-id priority]` hook
   reading ctx :result must handle both shapes."
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
                                  (if (and (= op operation) (= entity-id eid))
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

;; Delete Dataset → destroy! (scope check + short-circuit)
(defmethod mutate ["delete" dataset-xid -1]
  [ctx _]
  (assert-scope! "dataset:delete")
  (log/info {:id ::destroying-dataset
             :data {:action :destroying :subject :dataset
                    :dataset (:data ctx)}}
            "Destroying dataset")
  (reduced (assoc ctx :result (dataset/destroy! (:data ctx)))))

;; Delete Dataset Version → recall! (scope check + short-circuit)
(defmethod mutate ["delete" version-xid -1]
  [ctx _]
  (assert-scope! "dataset:delete")
  (log/info {:id ::recalling-dataset-version
             :data {:action :recalling :subject :dataset
                    :version (:data ctx)}}
            "Recalling dataset version")
  (reduced (assoc ctx :result (dataset/recall! (:data ctx)))))

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
  (execute-operation-chain (assert-map-selections ctx) "slice"))

(defmethod execute-operation "purge"
  [ctx]
  (execute-operation-chain (assert-map-selections ctx) "purge"))

(defmethod execute-operation "delete"
  [ctx]
  (execute-operation-chain ctx "delete"))

;; Analytics / arbitrary queries — ERD-aware SQL templates.
;; Covers aggregation, cross-entity joins, tree-rollups, and anything else
;; CRUD-shaped ops can't express. See synthigy.dataset.sql.template for the
;; placeholder DSL: `{entity.field}`, `{entity->rel.field}`, chain joins.
;;
;; DELIBERATELY not scope-gated (Robert, 2026-08-08): entity/relation RBAC
;; applies, but
;; deterministic aliases (e1, e2) make the PROJECTION unguardable — a data:sql
;; scope was
;; built and dropped, the cost (losing the console's SQL tab for ungranted
;; roles) outweighed
;; closing a hole attribute deny-lists never closed here anyway. Raw SQL sees
;; every
;; attribute of every readable entity; RLS still filters rows.
(defmethod execute-operation "sql-template"
  [{:keys [template params cached] :as ctx}]
  (assoc ctx :result (template/execute-template template params {:cached cached})))

;; Log query — one-shot read against the configured log backend.
;; synthigy.log.query/
;; validate-filter-map is the wire contract; coerce-log-filter lifts JSON string
;; shapes
;; (operator heads, level, paths, order-by) to keywords before validation runs.

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
  "Snake-case wire keys → kebab-case Clojure keys for the filter top level.
   JSON callers send snake; the validator expects kebab."
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

;; Store-aggregated (GROUP BY bucket) — returns ~N counts, never rows. Requires
;; bucket_ms.
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

;; Reads the LongAdder hot-path provider (synthigy.traffic) — traffic is
;; measured with
;; statistics at the source, never by querying log rows (Robert, 2026-07-22).
(defmethod execute-operation "traffic-stats"
  [{:keys [since until] :as ctx}]
  (assert-scope! "log:read")
  (doseq [[k v] [[:since since] [:until until]]]
    (when (and (some? v) (not (string? v)))
      (throw (ex-info (str "`traffic-stats` " (name k) " must be a string")
                      {:code "INVALID_LOG_FILTER" :field k}))))
  (assoc ctx :result (traffic/report {:since since :until until})))

;; get/set/clear the DB-backed routing overlay (synthigy.log.config); one
;; entrypoint, action-dispatched.
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
  (assoc ctx :result (transit/->transit (daccess/protect-model (dataset/deployed-model)))))

;; Augmented view (xid/euuid + audit attrs + reference-typed-attrs as
;; relations); modeler/canvas/deploy-drawer keep reading raw "deployed-model".
(defmethod execute-operation "runtime-model"
  [ctx]
  (assert-scope! "dataset:load")
  (assoc ctx :result (transit/->transit
                      (-> (dataset/deployed-model)
                          daccess/protect-model
                          runtime/build))))

(defmethod execute-operation "schema"
  [{:keys [entities] :as ctx}]
  (assert-scope! "schema:read" "dataset:load")
  ;; Runtime model so the projection covers everything the wire emits
  ;; (system/audit attrs,
  ;; reference-as-relation) — otherwise SDK resolvers can't name audit-attr xids
  ;; and XSQL
  ;; lint rejects legit created-on/modified-on references.
  (let [model (-> (dataset/deployed-model) daccess/protect-model runtime/build)]
    (assoc ctx :result (daccess/schema model entities))))

;; Codegen IR — XSQL program source → typed, language-neutral operation IR.
(defmethod execute-operation "describe"
  [{:keys [source params] :as ctx}]
  (assert-scope! "schema:read" "dataset:load")
  (when-not (string? source)
    (throw (ex-info "`describe` requires an :source XSQL program string"
                    {:code "BAD_OP" :rule "operation_shape"})))
  (let [model  (-> (dataset/deployed-model) daccess/protect-model runtime/build)
        schema (daccess/schema model)]
    (assoc ctx :result (codegen/describe schema source params))))

(defn deploy-payload
  "Normalize what a client sent into a dataset VERSION map. The export file a
   modeler writes is the artifact: a client hands over its bytes and gets
   deployed-or-error back, without knowing what is inside it. Accepts the whole
   export as a string, a version map whose `:model` is an export string, or an
   already-decoded version map."
  [data]
  (let [data (if (string? data) (transit/<-transit data) data)]
    (if (string? (:model data))
      (update data :model transit/<-transit)
      data)))

(defmethod execute-operation "deploy"
  [ctx]
  (assert-scope! "dataset:deploy")
  (let [data (deploy-payload (:data ctx))
        ;; Deployed versions are IMMUTABLE in CONTENT but CAN be redeployed —
        ;; that's how
        ;; rollback works (redeploy re-stamps deployed_on; rebuild-global-model
        ;; orders by it).
        ;; Only an attempt to ALTER a deployed version (differs from stored) is
        ;; rejected.
        vid (id/extract data)
        existing (when vid
                   (try
                     (dataset/get-entity :dataset/version
                                         {(id/key) vid}
                                         {:deployed nil (id/key) nil :model nil})
                     (catch Throwable _ nil)))]
    (when (:deployed existing)
      ;; Compare against what deploy! persists — legacy types normalized, ids
      ;; adapted, :version 1, transit round-trip so `=` is reliable. Never
      ;; compare a raw candidate: the store rewrites "avatar" to "json".
      (let [stored    (let [m (:model existing)]
                        (if (string? m) (transit/<-transit m) m))
            candidate (-> (:model data)
                          dataset.core/normalize-legacy-types
                          dataset/adapt-model-to-provider
                          (assoc :version 1)
                          transit/->transit
                          transit/<-transit)]
        (when (not= candidate stored)
          (throw (ex-info "Deployed version is immutable; create a new version to change the model."
                          {:code "IMMUTABLE_VERSION_ALTERED" :rule "deploy_immutable"})))))
    ;; deploy!'s ERDModel return used to hit the response key-transform, whose
    ;; :entities/
    ;; :relations keys collide with Dataset Version's same-named :many relations
    ;; ("String cannot be cast to Map$Entry") — clients re-fetch via the deploy
    ;; subscription anyway, so return a plain ack instead.
    (dataset/deploy! data)
    (assoc ctx :result {:deployed true
                        :version (:name data)
                        :dataset (id/extract (:dataset data))})))

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
  "Op names that emit `:synthigy.server.data/op-completed` on finish.
  Union of read + write data ops. Excludes meta-ops because their access
  is governed by separate scopes and they don't represent user data access:
    - deployed-model / runtime-model / schema — model introspection
    - deploy — admin operation
    - log-query / audit-query / history-query — observability surfaces
      (auditing the cockpit's own polls would make it watch itself)."
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
                     ;; Counts + bytes only, no values — verbatim payloads
                     ;; belong in the audit plug.
                     (and write?
                          (some? (:data op))) (-> (assoc :records-in
                                                         (write-records-in op))
                                                  (assoc :bytes-in
                                                         (write-bytes-in op)))
                     (:args op)              (assoc :args (:args op))
                     (:template op)          (assoc :template (:template op))
                     ;; Same key set the wire :error carries, so the log can't
                     ;; silently omit a field the client saw.
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
    ;; errors -> :error always; successful reads -> :info (first-class audit
    ;; fact);
    ;; successful writes -> :debug (audit plug already stores them
    ;; verbatim).
    (traffic/count! :ops)
    (cond
      (= status :error)
      (log/error {:id :synthigy.server.data/op-completed :data payload
                  :error throwable} msg)
      write?
      (log/debug {:id :synthigy.server.data/op-completed :data payload} msg)
      :else
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
  "Resolve one wire operation into the context execute-operation dispatches on.
   Public because embedded runs single ops without the batch scheduler."
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
                ;; ::op-shape carries the coerced :entity/:entity-id/:selections
                ;; so
                ;; transform-results can strip/shape without re-resolving;
                ;; dissoc'd before the wire.
                {:data (:result (execute-operation ctx))
                 :ok true
                 ::op-shape (select-keys ctx [:entity :entity-id :selections])})
              (catch clojure.lang.ExceptionInfo e
                (let [raw (ex-data e)
                      data (enrich-position-error raw op)
                      err  (merge {:message (ex-message e)
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
                                  (seq details) (assoc :details details))]
                    ;; TIMEOUT is a server-health signal -> warn; constraint
                    ;; codes are routine -> debug.
                    (if (= "TIMEOUT" code)
                      (log/warn {:id ::db-statement-timeout :error e}
                                "DB statement timed out")
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
                    ;; ::throwable rides to op-completed for the full
                    ;; stacktrace; the wire only sees the sanitized :error.
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
    (let [;; xsql ops are resolved to their real @verb up front — write-ops/audit-ops/
          ;; ops-requiring-entity key off :op, so otherwise an xsql-wrapped
          ;; write schedules
          ;; as a read and loses its audit row. Parse failures fall through
          ;; unresolved;
          ;; operation-context retries inside run-op's try/catch -> proper
          ;; XSQL_PARSE_ERROR.
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
   then apply key-fn if given. Public so embedded shapes identically."
  [key-fn op data]
  (cond
    ;; Silent-write marker ({:count n} from a returning:false sync/stack) —
    ;; not entity data, must not be stripped/key-transformed.
    (::count-only? (meta data))
    data

    (and (:entity op) (coll? data))
    (let [entity-id (or (:entity-id op) (sql-query/resolve-entity (:entity op)))
          ;; Only MAP selections drive stripping — an XSQL op's STRING
          ;; :selections would
          ;; reduce-kv over the string's characters and ClassCastException.
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

    ;; sql-template (and other entity-less ops) — apply key-fn to top-level
    ;; map keys only; no stripping (caller owns the shape).
    (and (nil? (:entity op)) key-fn (sequential? data))
    (mapv #(update-keys % key-fn) data)

    :else data))

(defn transform-results
  "Shape each operation's result: strip to :selections and apply key-fn; skips
   failed results and entity-less ops."
  [key-fn operations results]
  (mapv (fn [op result]
          ;; Prefer the coerced op-shape captured at execution — a raw "xsql"
          ;; wire op has
          ;; no :entity/:selections of its own (they fall out of parsing the
          ;; document).
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
                      ;; Claims ride from the TOKEN, not the impersonated user —
                      ;; scope checks are the client's.
                      :claims (:claims auth)}
                     operations)]
        {:results (transform-results key-fn operations results)}))))

