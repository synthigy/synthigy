(ns synthigy.server.data
  "Server-agnostic /data endpoint handler.

   Provides direct dataset operations via JSON, bypassing GraphQL.
   Designed for service-to-service communication where services use
   client credentials + acting_as for user impersonation.

   ## Wire Format

   Request:
   ```json
   POST /data
   Authorization: Bearer <token>
   Content-Type: application/json

   {
     \"acting_as\": \"user-euuid\",     // optional, for impersonation
     \"operations\": [
       {\"op\": \"search\", \"entity\": \"user\",
        \"args\": {\"_where\": {\"active\": {\"_eq\": true}}},
        \"selections\": {\"name\": null, \"email\": null}}
     ]
   }
   ```

   Response:
   ```json
   {\"results\": [
     {\"data\": [...], \"ok\": true},
     {\"error\": {\"message\": \"...\", \"code\": \"...\"}, \"ok\": false}
   ]}
   ```

   ## Read consistency

   Read operations are NOT snapshot-isolated. A search/get fans out into
   several SQL statements (root rows, `_count`, `_agg`, each related
   level) — these run autocommit, with no surrounding transaction, each
   at its own MVCC snapshot. So the nested pieces of one response reflect
   committed state within a few milliseconds of each other, not a single
   point in time: a parent's `_count` may briefly disagree with the
   length of its just-changed child list, self-healing on the next read.

   This is read-committed-per-statement — the standard contract for a
   data API, and what lets reads scale without pinning a connection per
   request. Writes are unaffected: a mutation runs in its own
   transaction. A caller needing a true consistent snapshot across a
   whole read should not rely on `/data` for that guarantee.

   See SDK.md for full specification."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.log.config :as log.config]
   [synthigy.log.query :as log.query]
   [patcho.lifecycle :as lifecycle]
   [synthigy.db :as db]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.codegen :as codegen]
   [synthigy.dataset.core :as dataset.core]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.runtime :as runtime]
   [synthigy.transit :as transit]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.dataset.sql.template :as template]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.json :as json]
   [synthigy.oauth.core :as oauth]
   [synthigy.server.auth :as auth]
   [synthigy.xsql.api :as xsql]
   [synthigy.xsql.program :as xsql-program]))

;;; ============================================================================
;;; Operation Dispatch
;;; ============================================================================

(defn- normalize-op
  "Normalize operation name to kebab-case.
   Accepts camelCase, PascalCase, snake_case, kebab-case, or spaces.
   e.g. \"deployedModel\" | \"DeployedModel\" | \"deployed_model\" | \"deployed-model\" → \"deployed-model\"
   Returns nil unchanged so the downstream BAD_OP gate can produce a
   typed error instead of an NPE."
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

(defn- coerce-args
  "Coerce JSON-boundary arg quirks before args reach the query layer.

  Shape gate: any `_`-prefixed top-level key not in `known-args-keys`
  is a typo (e.g. `_wheree` / `_lmit`); fail fast with `BAD_ARGS_SHAPE`.
  Cost is one `.startsWith` + one set lookup per top-level key on the
  happy path — negligible against a SQL build.

  _order_by: JSON objects lose insertion order (PersistentHashMap). Accept a
  vector of [field direction] pairs and convert to array-map so reduce-kv
  iterates in the caller's order. Map form (single-column, internal use)
  passes through unchanged."
  [args]
  (when (map? args)
    (doseq [[k _] args]
      (when (and (keyword? k)
                 (.startsWith (name k) "_")
                 (not (known-args-keys k)))
        (throw (ex-info
                 (str "Unknown args modifier " (pr-str k)
                      ". Supported: "
                      (clojure.string/join " " (sort (map name known-args-keys))))
                 {:code "BAD_ARGS_SHAPE"
                  :rule "args_modifier"
                  :modifier (name k)
                  :supported (mapv name (sort known-args-keys))})))))
  (if-let [ob (:_order_by args)]
    (cond
      ;; Map form (single-column, internal use) — pass through.
      (map? ob)
      args

      ;; Vector form — every entry must be a 2-element sequential
      ;; `[field direction]`. Without this gate, a flat shape like
      ;; `["name" "asc"]` silently destructures as iterators over the
      ;; *characters* of `"name"`/`"asc"` and crashes deep in the SQL
      ;; builder as an untranslated PSQLException (surfaces to clients
      ;; as INTERNAL_ERROR with no useful context).
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

(defn- write-op? [{:keys [op]}] (contains? write-ops op))

(def ^:private ops-requiring-entity
  "Op names for which `:entity` is mandatory. Caught at the op-shape gate
   in `operation-context` before any expensive work runs — failure mode
   `MISSING_ENTITY` rather than the late-binding crash that used to
   surface as `INTERNAL_ERROR`. Ops absent from this set (e.g.
   `sql-template`, `deployed-model`, `schema`, `log-query`) don't need
   an entity ref."
  #{"search" "get" "search-tree" "get-tree"
    "sync" "stack" "delete" "purge" "slice"})

;;; ============================================================================
;;; Error enrichment — turn DB-level errors into data-contract explanations
;;; ============================================================================
;;
;; The backend Translator (synthigy.db/translate-db-exception) maps a native
;; SQL exception to a backend-neutral :code + :details. The enricher below
;; layers on what the translator can't know — specifically, the operation
;; context and the deployed model — so the wire response describes the
;; data-contract violation rather than the SQL one.
;;
;; For SQLite, FK violations don't carry per-attribute context in the native
;; exception. The enricher walks the op's data + model to identify which
;; references the write attempted; one-ref ops get :attributes + :target_entity,
;; multi-ref ops get :candidates [{:attribute :target_entity}, ...].

(defn- find-fk-candidates
  "Given a write op and a deployed model, return the relations attempted
   in op[:data] as [{:attribute label :target_entity name} ...] sorted by
   attribute. Returns nil when context is insufficient (no model, no
   data, no entity match).

   Model relation labels are display-cased (`Mother`, `someAttr`); wire-data
   keys are snake_case. Both sides go through dk/normalize-key so they meet
   in canonical form."
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

(defn- enrich-fk-details
  "Replace bare :rule \"reference\" details with whatever model context we
   can recover from the operation data. Single attempted reference →
   :attributes + :target_entity. Multiple → :candidates list. None → leave
   :details as-is."
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

(defn- enrich-error-details
  "After the backend Translator returns, layer on operation/model context
   it couldn't see. Today this only enriches FK_VIOLATION; other codes
   already carry :rule + :entity + :attributes from the translator."
  [translated op]
  (let [data    (ex-data translated)
        details (or (:details data) {})]
    (case (:code data)
      "FK_VIOLATION" (enrich-fk-details details op)
      details)))

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

(defn- java-leaky-message?
  "True if `msg` matches any pattern in `java-leak-patterns`."
  [msg]
  (and (string? msg)
       (boolean (some #(re-find % msg) java-leak-patterns))))

(defn- safe-error-message
  "Sanitize an arbitrary exception's message for inclusion in an API
   response. Recognized structured errors (ex-info with `:code`) pass
   through untouched. Anything else with a Java/Clojure-leaky message
   gets replaced with a generic safe message; the original is left in
   the server log for operators.

   Returns `[clean-message original-message-or-nil]` — the second value
   lets the caller log the raw message at error level even when the
   wire response gets the sanitized version."
  [throwable]
  (let [raw    (ex-message throwable)
        clean? (and raw (not (java-leaky-message? raw)))]
    (if clean?
      [raw nil]
      ["Internal error processing operation. The full exception was logged server-side."
       raw])))

;;; ============================================================================
;;; Protected Entities
;;; ============================================================================

(defn- assert-scope!
  "Assert the current user has the required `scope` — or ANY of the
   `alternates` (e.g. a narrow scope with a broader legacy fallback).
   Throws FORBIDDEN naming the FIRST scope (the one to grant)."
  [scope & alternates]
  (when-not (some access/scope-allowed? (cons scope alternates))
    (throw (ex-info (str "Missing required scope: " scope)
                    {:code "FORBIDDEN" :scope scope}))))

;;; ============================================================================
;;; Operation Dispatch
;;; ============================================================================

(def ^:private dataset-entity-ids
  "Entity IDs for Dataset and Dataset Version (both ID formats)."
  {:dataset #{#uuid "a800516e-9cfa-4414-9874-60f2285ec330"
              "MkEtt4vQAsruc44MGypn63"}
   :version #{#uuid "d922edda-f8de-486a-8407-e62ad67bf44c"
              "Tp9i6E2m3kBNHtt7KwHgWX"}})

(defmulti execute-operation :op)

(defmethod execute-operation :default
  [ctx]
  (throw (ex-info (str "Unknown operation: '" (:op ctx) "'")
                  {:code "UNKNOWN_OP" :op (:op ctx)})))

;;; ============================================================================
;;; XSQL transport
;;;
;;; Operations whose `:selections` field is a STRING get the string
;;; compiled to wire shape via `synthigy.xsql.api/compile`. Read-shape
;;; ops (search/get/slice/purge) opt in by calling `coerce-xsql` at the
;;; top of their defmethod; write ops do not.
;;;
;;; If the XSQL produces top-level `_args` and the op didn't supply
;;; explicit `:args`, those args become the op's args. Explicit `:args`
;;; on the op wins where both are present.
;;; ============================================================================

(defn- xsql-diagnostics
  "Run XSQL syntax-only lint and decorate each diagnostic with
   1-based line/col coordinates."
  [^String source]
  (mapv (fn [d]
          (assoc d
                 :start (xsql/line-col source (:from d))
                 :end   (xsql/line-col source (:to d))))
        (xsql/lint source nil nil)))

(defn- offset->line-col
  "Convert a 0-based byte offset into a `{:line :col}` map (1-based).
   Mirrors `xsql/line-col` for non-XSQL sources (template SQL, log
   filter strings) so error responses everywhere can pinpoint a
   character position the same way."
  [^String source offset]
  (when (and (string? source) (integer? offset) (<= 0 offset (count source)))
    (loop [i 0 line 1 line-start 0]
      (if (>= i offset)
        {:line line :col (- offset line-start -1)}
        (if (= \newline (.charAt source i))
          (recur (inc i) (inc line) (inc i))
          (recur (inc i) line line-start))))))

(defn- enrich-position-error
  "When `error-data` carries a byte offset (`:position` for template
   errors; `:from`/`:to` for XSQL-style errors) AND the source string
   is available from the op, return enriched error data with `:line`
   `:col` (and `:start`/`:end` for ranges). No-op when fields are
   absent or the source isn't a string."
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

(defn- coerce-xsql
  "If the op's `:selections` field is a string, compile it as XSQL and
   replace `:selections` (and possibly `:args`) with the compiled wire
   shape. No-op when `:selections` is already a map / nil.

   When the op carries a `:params` map, it's threaded into the XSQL
   compiler to resolve `?name:type[]` placeholders. Missing required
   params or type mismatches throw `ex-info` with code
   `PARAM_MISSING` or `PARAM_TYPE_MISMATCH`.

   On parse failure throws an `ex-info` with code `XSQL_PARSE_ERROR`
   carrying a `:diagnostics` vector (each `{:message :from :to :start :end}`).
   The surrounding per-op `try` surfaces the message + code + diagnostics
   to the caller — no generic 500."
  [ctx]
  (let [sel (:selections ctx)]
    (if (string? sel)
      (let [diags (xsql-diagnostics sel)]
        (if (seq diags)
          (let [{:keys [message start end]} (first diags)
                {:keys [line col]} start]
            (throw (ex-info (str "XSQL parse error at line " line ", col " col ": " message)
                            {:code        "XSQL_PARSE_ERROR"
                             :diagnostics diags
                             :line        line
                             :col         col
                             :start       start
                             :end         end})))
          (let [op-str (some-> (:op ctx) name)
                params (:params ctx)
                {compiled-sel :selections compiled-args :args}
                (xsql/compile sel op-str params)]
            (cond-> (assoc ctx :selections compiled-sel)
              (and (seq compiled-args) (not (:args ctx)))
              (assoc :args compiled-args)))))
      ctx)))

(defn- coerce-xsql-op
  "An `xsql` operation carries a full XSQL operation DOCUMENT in `:xsql`
   (`@search recent\\n<rooted body>`). The `@verb` IS the real op; entity /
   selections / args (or `template`, or mutation `data`) come from the document.
   We parse it and replace the synthetic `:op \"xsql\"` with the real op so the
   normal pipeline (entity gate, id resolution, dispatch) runs unchanged.

   `:params` resolve the document's `?name` placeholders (and carry mutation
   record data, keyed by the data-variable name). This is the ONLY XSQL-aware
   wire shape — there is no XSQL inside a wire op's `:selections` (no complect)."
  [{:keys [xsql params] :as ctx}]
  (when-not (string? xsql)
    (throw (ex-info "`xsql` operation requires an :xsql document string"
                    {:code "BAD_OP" :rule "operation_shape"})))
  (let [diags    (xsql-program/lint xsql nil)
        errors   (filter #(= :error (:severity %)) diags)
        compiled (first (xsql-program/compile xsql params))]
    (when (seq errors)
      (let [{:keys [message from to]} (first errors)]
        (throw (ex-info (str "XSQL parse error: " message)
                        {:code "XSQL_PARSE_ERROR" :diagnostics (vec errors)
                         :from from :to to}))))
    (when (nil? compiled)
      (throw (ex-info "Empty XSQL operation document"
                      {:code "XSQL_PARSE_ERROR" :rule "operation_shape"})))
    (let [base (-> ctx (dissoc :xsql) (assoc :op (:op compiled)))]
      (cond
        (:mutate compiled)
        ;; `:data-var` is a string ("records"); wire `:params` keys are
        ;; keywordized by json/read-str, so look up by keyword (fall back to
        ;; the string for transit/edn callers that keep string keys).
        (let [dv (:data-var compiled)]
          (assoc base :entity (:entity compiled)
                      :data   (or (get params (keyword dv)) (get params dv))))

        (= "sql-template" (:op compiled))
        (assoc base :template (:sql compiled))

        :else
        (assoc base :entity     (:entity compiled)
                    :selections (:selections compiled)
                    :args       (:args compiled))))))

(defmethod execute-operation "search"
  [ctx]
  (let [{:keys [entity-id args] selection :selections :as ctx} (coerce-xsql ctx)]
    (assoc ctx :result (dataset/search-entity entity-id args selection))))

(defmethod execute-operation "get"
  [ctx]
  (let [{:keys [entity-id args] selection :selections :as ctx} (coerce-xsql ctx)]
    (assoc ctx :result (dataset/get-entity entity-id args selection))))

;; ========================================================================
;; Tree operations — recursive traversal via a named relation
;; ========================================================================
;;
;; search-tree — find entities matching :args + walk :on relation UP to
;;               ancestors. Returns [match..., match-ancestors...].
;; get-tree    — from an explicit :root, return root + descendants
;;               reachable via :on (reverse FK lookup).
;;
;; Aggregation across a tree (or any non-trivial analytics shape) is done
;; via the `sql-template` op — see below.
;;
;; Request shape:
;;   {"op": "search-tree" | "get-tree",
;;    "entity": "<entity-id-or-xid>",
;;    "on": "<relation-name>",        ; keyword name, e.g. "father"
;;    "root": "<starting-xid>",       ; get-tree only
;;    "args": {"_where": ...},        ; search-tree only
;;    "selections": {...}}

(defmethod execute-operation "search-tree"
  [{:keys [entity-id on args] selection :selections :as ctx}]
  (when-not on
    (throw (ex-info "search-tree requires :on (relation name)"
                    {:code "MISSING_ON"})))
  (assoc ctx :result
         (dataset/search-entity-tree entity-id (keyword on) args selection)))

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
  "Extensible hook chain for write operations. Dispatches on [op entity-id priority].

   Priority determines execution order:
     negative  — pre-hooks, run before the default handler (sorted ascending)
     ##Inf     — the default handler (actual mutation)
     positive  — post-hooks, run after the default handler (sorted ascending)

   Use `reduced` in a pre-hook to short-circuit and skip all subsequent steps."
  (fn [{:keys [op entity-id]} priority]
    [op entity-id priority]))

(defmethod mutate :default
  [{:keys [op entity-id args data] selection :selections :as ctx} _]
  (assoc ctx :result
         (case op
           "sync"      (dataset/sync-entity entity-id data)
           "stack"     (dataset/stack-entity entity-id data)
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

;;; ============================================================================
;;; Dataset / Dataset Version lifecycle hooks
;;; ============================================================================

;; Delete Dataset → destroy! (scope check + short-circuit)
(doseq [eid (:dataset dataset-entity-ids)]
  (defmethod mutate ["delete" eid -1]
    [ctx _]
    (assert-scope! "dataset:delete")
    (log/info {:id ::destroying-dataset
               :data {:dataset (:data ctx)}}
              "Destroying dataset")
    (reduced (assoc ctx :result (dataset/destroy! (:data ctx))))))

;; Delete Dataset Version → recall! (scope check + short-circuit)
(doseq [eid (:version dataset-entity-ids)]
  (defmethod mutate ["delete" eid -1]
    [ctx _]
    (assert-scope! "dataset:delete")
    (log/info {:id ::recalling-dataset-version
               :data {:version (:data ctx)}}
              "Recalling dataset version")
    (reduced (assoc ctx :result (dataset/recall! (:data ctx))))))

;; Purge Dataset — forbidden
(doseq [eid (:dataset dataset-entity-ids)]
  (defmethod mutate ["purge" eid -1]
    [_ _]
    (throw (ex-info "Purge not allowed on Dataset. Use delete instead."
                    {:code "FORBIDDEN_OP"}))))

;; Purge Dataset Version — forbidden
(doseq [eid (:version dataset-entity-ids)]
  (defmethod mutate ["purge" eid -1]
    [_ _]
    (throw (ex-info "Purge not allowed on Dataset Version. Use delete instead."
                    {:code "FORBIDDEN_OP"}))))

;;; ============================================================================
;;; Execute operation methods
;;; ============================================================================

(defmethod execute-operation "sync"
  [ctx]
  (execute-operation-chain ctx "sync"))

(defmethod execute-operation "stack"
  [ctx]
  (execute-operation-chain ctx "stack"))

(defmethod execute-operation "slice"
  [ctx]
  (execute-operation-chain (coerce-xsql ctx) "slice"))

(defmethod execute-operation "purge"
  [ctx]
  (execute-operation-chain (coerce-xsql ctx) "purge"))

(defmethod execute-operation "delete"
  [ctx]
  (execute-operation-chain ctx "delete"))

;; Analytics / arbitrary queries — ERD-aware SQL templates.
;; Covers aggregation, cross-entity joins, tree-rollups, and anything else
;; CRUD-shaped ops can't express. See synthigy.dataset.sql.template for the
;; placeholder DSL: `{Entity.field}`, `{Entity -> rel.field}`, chain joins.
(defmethod execute-operation "sql-template"
  [{:keys [template params cached] :as ctx}]
  (assoc ctx :result (template/execute-template template params {:cached cached})))

;; ============================================================================
;; Log query — one-shot read against the configured log query backend.
;;
;; Wire shape:
;;   {"op": "log-query",
;;    "filter": {"where": {"level": "error",
;;                          "ns":    ["starts-with", "synthigy.oauth"],
;;                          "data":  {"action": "deployed"}},
;;               "since": "1h",
;;               "limit": 100,
;;               "order_by": ["inst", "desc"]}}
;;
;; The validator in `synthigy.log.query/validate-filter-map` is the wire
;; contract — anything it rejects is a 4xx-shaped error. JSON-only callers
;; can't send keywords directly, so `coerce-log-filter` lifts operator
;; heads, level values, paths and order-by elements from string→keyword
;; before validation runs. Nothing else is coerced; numeric and string
;; argument values pass through unchanged.
;; ============================================================================

(defn- coerce-log-where-value
  "Coerce a single :where value from JSON shape to the validate-filter-map
   contract. Tuple vectors get their operator head keywordized; :in / :not_in
   wrap their args in a set; :matches compiles its String to a Pattern.
   Bare values for the :level field also keywordize for convenience."
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

(defn- coerce-log-where
  "Walk :where, expanding nested {:data {…}} / {:ctx {…}} into path-keyed
   entries the validator expects (`[:data :action]` instead of nested maps).
   Two levels only — `data.error.class`-style triple paths are a Phase-N
   need, not Phase 1."
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
   :group_by :group-by})

(defn- coerce-log-filter
  "Lift the wire filter map into the shape `validate-filter-map` accepts:
   rename snake-case top-level keys (`order_by` → `order-by`), keywordize
   operator heads in :where tuples, expand :data/:ctx nested objects into
   path keys, keywordize :order-by elements."
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

;; Runtime log-config — get/set/clear the DB-backed routing overlay
;; that `synthigy.log.config` applies via `log/apply-routing!`. The same
;; entrypoint serves all three actions; the FE switches on `:action`.
;;
;; Wire shape:
;;   {"op":"log-config","action":"get"}
;;   {"op":"log-config","action":"set",
;;    "config":{"root_level":"info",
;;              "ns_overrides":[{"pattern":"synthigy.dataset.sql.query","level":"trace"}]}}
;;   {"op":"log-config","action":"clear"}
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

;; Runtime-model: the augmented view consumed by the data console (and
;; future SDK introspection). Carries xid/euuid + audit attrs +
;; reference-typed-attrs surfaced as relations. Composed from the same
;; raw deployed model — the modeler/canvas/deploy-drawer continue to
;; read "deployed-model" (raw); only consumers that want the runtime
;; picture read this one. See dataset.runtime/build.
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
  ;; Source from the runtime model so the projection covers everything
  ;; the wire actually emits — system attrs (xid/euuid), audit attrs
  ;; (created/modified per entity audit config) and reference-as-relation
  ;; expansions. Without this, SDK resolvers can't name audit-attr xids
  ;; that arrive on delta envelopes, and XSQL lint would reject queries
  ;; that legitimately reference `created-on` / `modified-on` / etc.
  (let [model (-> (dataset/deployed-model) daccess/protect-model runtime/build)]
    (assoc ctx :result (daccess/schema model entities))))

;; Codegen IR — XSQL program source → typed, language-neutral operation IR.
;; The server parses + type-derives once; per-language emitters render the IR.
;; Wire: {"op":"describe", "source":"<xsql program>", "params":{…}}.
(defmethod execute-operation "describe"
  [{:keys [source params] :as ctx}]
  ;; codegen introspection — `schema:read` is the scope to grant an app
  ;; client for `gen --pull`; `dataset:load` (model tooling) also passes.
  (assert-scope! "schema:read" "dataset:load")
  (when-not (string? source)
    (throw (ex-info "`describe` requires an :source XSQL program string"
                    {:code "BAD_OP" :rule "operation_shape"})))
  (let [model  (-> (dataset/deployed-model) daccess/protect-model runtime/build)
        schema (daccess/schema model)]
    (assoc ctx :result (codegen/describe schema source params))))

(defmethod execute-operation "deploy"
  [ctx]
  (assert-scope! "dataset:deploy")
  (let [data (:data ctx)
        ;; Deserialize model from transit if present
        data (if (string? (:model data))
               (update data :model transit/<-transit)
               data)]
    ;; `deploy!` is a command (migrate schema + save-model! + :model/deployed
    ;; notify); its ERDModel return is consumed by no caller. Returning it here
    ;; fed a structured `{string->record}` model to the response key-transform,
    ;; whose `:entities`/`:relations` keys collide with Dataset Version's
    ;; same-named :many relations → "String cannot be cast to Map$Entry". Clients
    ;; ignore this body and re-fetch via the deploy subscription, so return a
    ;; plain ack instead.
    (dataset/deploy! data)
    (assoc ctx :result {:deployed true
                        :version (:name data)})))

(defn ensure-tree-on-relation
  "Tree ops walk a self-FK `on` relation; the response must carry that
   relation on every record so consumers can reconstruct parent→child
   links. If the caller didn't list it in `:selections`, inject an empty
   sub-selection (`selection->schema` seeds the id key, so the FK comes
   back automatically; the post-strip preserves it because the key is now
   in the selection).

   Skips XSQL string selections — those carry their own selection text
   and shouldn't be force-mutated. (Console-side XSQL compiler can add
   the on-relation pre-flight if needed; this function only touches the
   already-compiled wire shape.)"
  [op]
  (if-let [on (and (#{"search-tree" "get-tree"} (:op op)) (:on op))]
    (let [k   (keyword on)
          sel (:selections op)]
      (cond
        (or (nil? sel) (map? sel))
        (cond-> op
          (not (contains? (or sel {}) k))
          (assoc-in [:selections k] [{:selections nil}]))
        ;; XSQL string or other shape — leave alone
        :else op))
    op))

;;; ============================================================================
;;; /data op completion — one info row per audited /data op (success or
;;; failure, read or write). Always-on observability for the cockpit's SQL
;;; lens. Reads carry selection shape + predicate key paths (no values —
;;; those land in trace via the SQL builder emit points in
;;; `synthigy.dataset.sql.query`). Writes carry op + duration only; the data
;;; side is covered by the audit substrate (synthigy.audit).
;;; ============================================================================

(def ^:private audit-ops
  "Op names that emit `:synthigy.server.data/op-completed` on finish.
  Union of read + write data ops. Excludes meta-ops because their access
  is governed by separate scopes and they don't represent user data access:
    - deployed-model / runtime-model / schema — model introspection
    - deploy — admin operation
    - log-query / audit-query / history-query — observability surfaces
      (auditing the cockpit's own polls would make it watch itself)."
  (into write-ops #{"search" "get" "search-tree" "get-tree" "sql-template"}))

(defn- selection-shape
  "Compact tree representation of a selections map for info-level logging.
   Drops scalar values; preserves the relation tree and per-level scalar
   count. Returns nil for non-map shapes (XSQL string selections etc.)."
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

(defn- predicate-keys
  "Return the set of key paths through a filter map to leaf operator
   values. Strips all values — only key paths preserved. Used at info
   level so we can see which columns were filtered without storing
   user-supplied predicate values (PII / cost). Values land in trace
   via the SQL builder emit points."
  [filter-map]
  (letfn [(walk [path m]
            (cond
              (map? m)        (mapcat (fn [[k v]] (walk (conj path k) v)) m)
              (sequential? m) (mapcat #(walk path %) m)
              :else           [path]))]
    (->> (walk [] filter-map) (filter seq) set)))

(defn- write-records-in
  "How many records the write op carried in. Vector `:data` → count;
   single map (e.g. `delete {:xid …}`) → 1; missing → nil. Predicate-
   driven writes (`purge`, filter-based deletes) have no record count
   from input — returns nil and the lens just shows the row-count
   on the result side."
  [op]
  (let [d (:data op)]
    (cond
      (sequential? d) (count d)
      (map? d)        1
      :else           nil)))

(defn- write-bytes-in
  "Approximate serialized byte size of the write op's input. Uses
   `pr-str` length as a cheap proxy — not literal wire bytes, just an
   order-of-magnitude figure for the lens's `N rec · M KB` column.
   Volume-safe even for 100k-row imports: this is a single integer
   in the log row, never the payload itself."
  [op]
  (when-let [d (:data op)]
    (try (count (.getBytes ^String (pr-str d) "UTF-8"))
         (catch Throwable _ nil))))

(defn- audit-op-completed!
  "Emit `:synthigy.server.data/op-completed` info row for one /data op.
   Always fires (success or failure, read or write) for ops in `audit-ops`.
   `request-id` / `user-xid` flow in via `log/with-ctx` from the request-id
   middleware and are auto-promoted to top-level signal columns by the log
   pipeline — `:data.user-xid` is also included so the lens can pivot on it
   even when transport-side ctx enrichment is incomplete."
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
                     ;; Volume capture for writes — counts + bytes only, NO
                     ;; values. Safe to emit always; bounded regardless of
                     ;; input size. Verbatim payloads belong in the audit
                     ;; substrate, not the log stream.
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
                                                     [:code :message :rule :hint
                                                      :diagnostics :path
                                                      :entity :relation
                                                      :attribute :argument
                                                      :modifier :supported
                                                      :line :col :start :end])))
        msg        (cond
                     (= status :error)
                     (str (if write? "write" "read") " failed"
                          (when-let [code (:code err)] (str " (" code ")")))
                     :else
                     (if write? "write finished" "read finished"))]
    (if (= status :error)
      (log/error {:id :synthigy.server.data/op-completed :data payload} msg)
      (log/info  {:id :synthigy.server.data/op-completed :data payload} msg))))

(defn- maybe-audit-op! [op started-ms result]
  (when (contains? audit-ops (:op op))
    (try (audit-op-completed! op started-ms result)
         (catch Throwable _))))

(defn- execute-operations
  "Execute operations with writes-first ordering and read parallelism.

   Writes execute sequentially (preserving order), then reads execute
   in parallel via futures with conveyed bindings. Results are returned
   in the original request order."
  [ctx operations]
  (letfn [(operation-context
            [op]
            (as-> (merge op ctx) ctx
              ;; An `xsql` operation is a full XSQL document — parse it so the
              ;; @verb becomes the real op and entity/selections/args fall out.
              ;; This is the ONLY place XSQL source meets the wire; a regular
              ;; wire op never carries XSQL in :selections (no complecting).
              (if (= "xsql" (:op ctx)) (coerce-xsql-op ctx) ctx)
              ;; Cheap shape gates — two map lookups + two set lookups; runs
              ;; before any walk so a missing `:op` / `:entity` fails fast
              ;; with a typed code rather than crashing late as INTERNAL_ERROR.
              (do (when (nil? (:op ctx))
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
              (update ctx :args coerce-args)
              (if (:entity ctx)
                (assoc ctx :entity-id (sql-query/resolve-entity (:entity ctx)))
                ctx)))
          (run-op
            [op]
            (try
              {:data (:result (execute-operation (operation-context op)))
               :ok true}
              (catch clojure.lang.ExceptionInfo e
                (let [raw (ex-data e)
                      data (enrich-position-error raw op)
                      err  (cond-> {:message (ex-message e)
                                    :code    (or (:code data) "OPERATION_ERROR")}
                             (seq (:diagnostics data)) (assoc :diagnostics (:diagnostics data))
                             (:hint data)              (assoc :hint        (:hint data))
                             (seq (:available data))   (assoc :available   (:available data))
                             (seq (:path data))        (assoc :path        (:path data))
                             (:entity data)            (assoc :entity      (:entity data))
                             (:relation data)          (assoc :relation    (:relation data))
                             (:operator data)          (assoc :operator    (str (:operator data)))
                             (:line data)              (assoc :line        (:line data))
                             (:col data)               (assoc :col         (:col data))
                             (:start data)             (assoc :start       (:start data))
                             (:end data)               (assoc :end         (:end data))
                             ;; Wire-triage fields added 2026-05-30 so the new typed codes
                             ;; (UNKNOWN_ATTRIBUTE, BAD_ARGS_SHAPE, MISSING_ENTITY, BAD_OP)
                             ;; surface their context to the client without being silently
                             ;; stripped by this envelope builder.
                             (:rule data)              (assoc :rule        (:rule data))
                             (:attribute data)         (assoc :attribute   (:attribute data))
                             (:modifier data)          (assoc :modifier    (:modifier data))
                             (seq (:supported data))   (assoc :supported   (:supported data))
                             (:argument data)          (assoc :argument    (:argument data))
                             (:op data)                (assoc :op          (:op data)))]
                  {:error err :ok false}))
              (catch java.sql.SQLException e
                (if-let [translated (some-> db/*db* (db/translate-db-exception e))]
                  (let [data    (ex-data translated)
                        code    (:code data)
                        details (enrich-error-details translated op)
                        err     (cond-> {:message (ex-message translated)
                                         :code    code}
                                  (seq details) (assoc :details details))]
                    ;; TIMEOUT is a server-health signal (lock contention,
                    ;; statement_timeout, query too slow) — surface it at warn
                    ;; so it shows up in dashboards. The constraint codes are
                    ;; routine user-data signals → debug.
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
                    {:error {:message clean :code "INTERNAL_ERROR"}
                     :ok false})))
              (catch Throwable e
                (let [[clean raw] (safe-error-message e)]
                  (log/error! {:id ::operation-failed
                               :msg "Operation failed"
                               :data {:op (:op op) :entity (:entity op)
                                      :raw-message raw}}
                              e)
                  {:error {:message clean :code "INTERNAL_ERROR"}
                   :ok false}))))
          (process-operation
            [op]
            (let [started (System/currentTimeMillis)
                  result  (run-op op)]
              (maybe-audit-op! op started result)
              result))]
    (let [;; Tag each operation with its original index
          indexed (map-indexed (fn [i op] [i (update op :op normalize-op)]) operations)
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

;;; ============================================================================
;;; Body Preservation Middleware
;;; ============================================================================

(defn wrap-preserve-body
  "Ring middleware that preserves the raw body for JSON endpoints.
   Must be applied BEFORE wrap-params which consumes the InputStream.
   Only activates for POST requests with application/json content type."
  [handler]
  (fn [request]
    (if (and (= :post (:request-method request))
             (:body request)
             (some-> (get-in request [:headers "content-type"])
                     (.startsWith "application/json")))
      (let [raw (if (string? (:body request))
                  (:body request)
                  (slurp (:body request)))]
        (handler (assoc request
                        :raw-body raw
                        :body (java.io.ByteArrayInputStream. (.getBytes raw "UTF-8")))))
      (handler request))))

;;; ============================================================================
;;; Content Negotiation
;;; ============================================================================

(defn- content-type
  "Extract content type from request, ignoring charset params."
  [request]
  (some-> (get-in request [:headers "content-type"])
          (str/split #";")
          first
          str/trim
          str/lower-case))

(defn- accept-type
  "Extract preferred Accept type from request."
  [request]
  (some-> (get-in request [:headers "accept"])
          (str/split #",")
          first
          str/trim
          str/lower-case))

(defn- read-body-str
  "Read request body as string."
  [request]
  (let [body (or (:raw-body request) (:body request))]
    (when body
      (if (string? body)
        body
        (slurp (io/reader body))))))

(defn parse-request-body
  "Parse request body based on Content-Type.
   Supports: application/json, application/transit+json, application/edn."
  [request]
  (try
    (when-let [body-str (read-body-str request)]
      (when-not (str/blank? body-str)
        (case (content-type request)
          "application/transit+json" (transit/<-transit body-str)
          "application/edn" (edn/read-string body-str)
              ;; Default: JSON
          (json/read-str body-str))))
    (catch Exception e
      (log/debug {:id ::body-parse-failed :error e}
                 "Failed to parse request body")
      nil)))

(defn format-response
  "Create Ring response with content negotiation based on Accept header.
   Supports: application/json, application/transit+json, application/edn."
  [request status body]
  (case (accept-type request)
    "application/transit+json"
    {:status status
     :headers {"Content-Type" "application/transit+json"}
     :body (transit/->transit body)}

    "application/edn"
    {:status status
     :headers {"Content-Type" "application/edn"}
     :body (pr-str body)}

    ;; Default: JSON
    {:status status
     :headers {"Content-Type" "application/json"}
     :body (json/write-str body)}))

(defn json-response
  "Create a JSON Ring response. Used by subscription and other modules
   that don't need content negotiation."
  [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/write-str body)})

;;; ============================================================================
;;; Schema Handler
;;; ============================================================================

(defn schema-handler
  "Ring handler for GET /schema.

  Returns the IAM-filtered model schema. Bearer token required (skipped
  when IAM is not started, e.g. in dev with SYNTHIGY_IAM_ALLOW_PUBLIC=true).

  Query param: ?entities=user,human  — comma-separated kebab-case entity
  names to filter the response. Omit for the full schema.

  Response shape:
    { \"id-key\": \"xid\",
      \"entities\": { \"user\": { \"name\", \"attributes\", \"relations\", \"constraints\" } } }"
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (try
        (access/with-principal (:principal iam)
          (let [;; Runtime model (deployed + identity + audit + ref relations) —
                ;; same source as `op:"schema"` so HTTP and /data agree.
                model         (-> (dataset/deployed-model)
                                  daccess/protect-model
                                  runtime/build)
                raw-entities  (get-in request [:query-params "entities"])
                entity-filter (when (seq raw-entities)
                                (->> (str/split raw-entities #",")
                                     (map str/trim)
                                     (filter seq)))
                ;; `?key_format=snake|kebab|camel` lets the consumer pick name
                ;; casing (default snake — matches the wire). /schema is JSON data,
                ;; not the strict XSQL grammar, so casing here is a preference.
                key-fmt       (get-in request [:query-params "key_format"])
                name-fn       (get daccess/name-fn-for key-fmt daccess/*name-fn*)
                result        (binding [daccess/*name-fn* name-fn]
                                (daccess/schema model entity-filter))
                ;; Echo the deploy drift-stamp so `pull` can stamp the generated
                ;; artifact atomically — from the same response it generated from
                ;; (avoids a redeploy racing between a separate version lookup).
                vinfo         (dataset/deployed-version-info)
                result        (cond-> result
                                vinfo (assoc :version     (:version vinfo)
                                             :version-id  (:version-id vinfo)
                                             :deployed-at (:deployed-at vinfo)))]
            (json-response 200 result)))
        (catch Throwable e
          (json-response 500 {:error {:message (ex-message e)
                                      :code    "INTERNAL_ERROR"}}))))))

(defn lint-source
  "Lint an XSQL source string. Pure-ish — reads `(dataset/deployed-model)`
   under the bound `access/*principal*` to get the IAM-projected schema,
   but takes no side effects. Returns a vector of enriched diagnostics
   `[{:severity, :message, :from, :to, :start, :end}, ...]`.

   `entity` (kebab-case name) is optional — when supplied AND present in
   the projected schema, schema-aware checks run; otherwise only syntax-
   level + param-ref diagnostics are reported.

   `op` defaults to `\"search\"`."
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

(defn lint-handler
  "Ring handler for POST /lint — schema-aware XSQL diagnostics.

  Request body:
    { \"source\":  \"_args (active = true)\\n\\nname\\n\",
      \"entity\":  \"user\",     // optional — kebab-case root entity name
      \"op\":      \"search\" }  // optional, defaults to \"search\"

  Response:
    { \"diagnostics\": [
        { \"severity\": \"error\",
          \"message\":  \"...\",
          \"from\": 12, \"to\": 16,
          \"start\": {\"line\": 2, \"col\": 5},
          \"end\":   {\"line\": 2, \"col\": 9} }, ...] }

  Schema is the same IAM-filtered model `GET /schema` returns, so the
  same Authorization is required.

  All actual lint computation is in [[lint-source]] — keep this thin so
  it stays straightforward to test against."
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (let [body (parse-request-body request)]
        (if-not body
          (json-response 400 {:error {:message "Invalid or missing JSON body"
                                      :code    "INVALID_BODY"}})
          (try
            (access/with-principal (:principal iam)
              (let [opts {:source (or (:source body) (get body "source"))
                          :entity (or (:entity body) (get body "entity"))
                          :op     (or (:op body) (get body "op"))}
                    diags (lint-source opts)]
                (json-response 200 {:diagnostics diags})))
            (catch Throwable e
              (log/error! {:id ::lint-failed} e)
              (json-response 500 {:error {:message (ex-message e)
                                          :code    "INTERNAL_ERROR"}}))))))))

;;; ============================================================================
;;; Acting-As Resolution
;;; ============================================================================

(defn- resolve-acting-as
  "Resolve the acting_as user from the request body.

   Enforces:
   - user exists (else USER_NOT_FOUND)
   - user is not explicitly deactivated (:active = false → USER_INACTIVE)

   The check matches the client-inactive check: fires only on EXPLICIT
   `false`, not on nil / missing. That keeps the behavior backward-
   compatible — the codebase doesn't default `:active` to `true` on user
   creation (see iam/set-user), so legacy records with nil `:active`
   would otherwise be rejected. Deactivation is a deliberate flip from
   true → false, not a missing-field state.

   Trusted-client check lives in the caller, before invoking this."
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

;;; ============================================================================
;;; Key Transformation
;;; ============================================================================

(defn- strip-eid
  "Remove the internal `:_eid` (Postgres serial PK) from a result value,
   recursively. It's a storage detail — never the client's business. Reads
   already drop it via `strip-to-selection` (it's never a selected key); this
   covers the no-selection paths (writes: sync/stack return the record with
   `:_eid` from the RETURNING clause)."
  [v]
  (cond
    (map? v)        (reduce-kv (fn [m k val]
                                 (if (= k :_eid) m (assoc m k (strip-eid val))))
                               {} v)
    (sequential? v) (mapv strip-eid v)
    :else           v))

(defn- transform-results
  "Shape each operation's result:
   1. strip — drop keys not in the op's :selections (keeps id key).
      Always runs when the op has a :selections; handles the auto-included
      empty recursion/reference maps (e.g. :mother {}, :father {}) that
      the query engine emits regardless of what was asked for.
   2. key-format — if key-fn is non-nil, apply kebab/camel/snake transform
      schema-aware, recursing through relations.
   No-selection paths (writes) instead get `:_eid` stripped — selection paths
   already drop it.

   Skips failed results and entity-less ops (schema introspection, etc.)."
  [key-fn operations results]
  (mapv (fn [op result]
          ;; Only shape collection results — scalar results (delete → bool,
          ;; count → number) carry no keys to strip/transform and would
          ;; otherwise blow up the key-format walker.
          (cond
            (and (:ok result) (:entity op) (coll? (:data result)))
            (let [entity-id (sql-query/resolve-entity (:entity op))
                  ;; Only MAP selections can drive stripping. XSQL ops carry a
                  ;; STRING `:selections` whose columns the compiled SQL already
                  ;; projected — skip strip (else `strip-to-selection` reduce-kv's
                  ;; over the string's chars → ClassCastException). `_eid` still
                  ;; gets stripped via the no-sel branch below.
                  sel (when (map? (:selections op))
                        (dataset/normalize-selection (:selections op)))
                  strip #(dataset/strip-to-selection entity-id sel %)
                  key-xf #(dk/transform-result entity-id key-fn % sel)
                  xf (cond
                       (and sel key-fn) (comp key-xf strip)
                       sel strip
                       key-fn (comp strip-eid key-xf)
                       :else strip-eid)
                  data (:data result)]
              (assoc result :data
                     (if (sequential? data) (mapv xf data) (xf data))))

            ;; sql-template (and other entity-less ops) — apply key-fn to
            ;; top-level map keys only; no stripping (caller owns the shape).
            (and (:ok result) (nil? (:entity op)) key-fn (sequential? (:data result)))
            (assoc result :data (mapv #(update-keys % key-fn) (:data result)))

            :else result))
        operations results))

;;; ============================================================================
;;; Handler
;;; ============================================================================

(defn handler
  "Ring handler for the /data endpoint.

   Authentication:
   - Bearer token required (validated against active tokens)
   - If acting_as present: service must be trusted, user must be active
   - If no acting_as: executes as the token owner

   Args:
     request - Ring request map

   Returns:
     Ring response map"
  [request]
  (let [;; Authenticate service/user token (skip if IAM not started or public allowed)
        iam-active? (lifecycle/started? :synthigy/iam)
        iam (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (format-response request 401 {:error {:message "Unauthorized"
                                            :code "UNAUTHORIZED"}})
      ;; Parse body
      (let [body (parse-request-body request)]
        (if-not body
          (format-response request 400 {:error {:message "Invalid or missing JSON body"
                                                :code "INVALID_BODY"}})
          (let [{:keys [acting_as key_format operations]} body
                key-fn (dk/format->key-fn (or key_format "snake"))]
            (if (empty? operations)
              (format-response request 400 {:error {:message "No operations provided"
                                                    :code "NO_OPERATIONS"}})
              ;; Resolve user context
              (try
                (let [;; Each guard returns a distinct error code so callers
                      ;; can tell misconfiguration apart from ops-disabling:
                      ;;
                      ;;   CLIENT_NOT_FOUND    — token references a gone client
                      ;;   CLIENT_INACTIVE     — client :active explicitly false
                      ;;   PUBLIC_CLIENT_FORBIDDEN — public clients never
                      ;;                         impersonate, even if flagged
                      ;;                         trusted (browser / SPA can't
                      ;;                         keep secrets; impersonation
                      ;;                         from there is unsafe by design)
                      ;;   NOT_TRUSTED         — confidential but not explicitly
                      ;;                         trusted for impersonation
                      ;;
                      ;; Checked in order of severity; first match wins.
                      _ (when acting_as
                          (let [client_id (get-in iam [:claims :client_id])
                                client (when client_id (oauth/get-client client_id))
                                public? (boolean (#{:public "public"} (:type client)))
                                trusted? (boolean (get-in client [:settings "trusted"]))]
                            (cond
                              (and client_id (nil? client))
                              (throw (ex-info "Client not found"
                                              {:code "CLIENT_NOT_FOUND"
                                               :client-id client_id}))

                              (and client (false? (:active client)))
                              (throw (ex-info "Client is inactive"
                                              {:code "CLIENT_INACTIVE"
                                               :client-id client_id}))

                              (and client public?)
                              (throw (ex-info "Public clients cannot impersonate users"
                                              {:code "PUBLIC_CLIENT_FORBIDDEN"
                                               :client-id client_id}))

                              (and client_id (not trusted?))
                              (throw (ex-info "Client not authorized for impersonation"
                                              {:code "NOT_TRUSTED"
                                               :client-id client_id})))))
                      user-ctx (if acting_as
                                 ;; Impersonation: load acting_as user
                                 (resolve-acting-as acting_as)
                                 ;; Direct: use token owner
                                 iam)]
                  ;; Bind IAM context and execute operations
                  (access/with-principal (:principal user-ctx)
                    (let [;; Tree ops need their on-relation projected so
                          ;; consumers can rebuild the chain. Augment once
                          ;; here so both execute and the post-strip see
                          ;; the same selection shape.
                          operations (mapv ensure-tree-on-relation operations)
                          results (execute-operations
                                   {:principal (:principal user-ctx)
                                    :claims (:claims iam)}
                                   operations)
                          ;; Always shape results — strip to :selections (and
                          ;; optionally apply key-format). Skipping used to
                          ;; be keyed on key-fn but stripping is valuable
                          ;; on its own (the query engine emits phantom
                          ;; relation keys even when caller didn't ask for
                          ;; them).
                          results (transform-results key-fn operations results)]
                      (format-response request 200 {:results results}))))
                (catch clojure.lang.ExceptionInfo e
                  (let [code (:code (ex-data e))
                        status (if (= code "USER_NOT_FOUND") 404 403)]
                    (format-response request status
                                     {:error {:message (ex-message e)
                                              :code code}})))
                (catch Throwable e
                  ;; Last-resort safety net for anything that escaped the
                  ;; per-op handler (auth/body resolution, ring-layer
                  ;; surprises). Log the raw exception server-side; return
                  ;; a sanitized message so Java internals never reach
                  ;; the wire.
                  (let [[clean raw] (safe-error-message e)]
                    (log/error! {:id ::data-handler-uncaught
                                 :msg "Uncaught exception in /data handler"
                                 :data {:raw-message raw}}
                                e)
                    (format-response request 500
                                     {:error {:message clean
                                              :code "INTERNAL_ERROR"}})))))))))))
