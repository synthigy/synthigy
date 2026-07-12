(ns synthigy.log.query
  "Filter-map spec + dispatch into the bound observability store.

  The CONTRACT (filter-map shape, operator vocabularies, validation rules)
  lives here and is shared by every store impl. Execution dispatches into
  whatever `synthigy.log.store/*log-store*` is bound — the store's `search`
  method consumes the validated filter-map and returns wire-schema-v1 rows
  (or a scalar count, or a vector of group buckets).

  `*log-store*` is defonce'd at protocol-ns load to a fresh
  `synthigy.log.store/RingLogStore` — so query works from JVM start, no
  `install!` step required. Starting `:synthigy/observability` swaps the
  dynvar to a durable backend (DuckDB, ClickHouse) and drains the ring;
  reads transparently follow. Stop rebinds a fresh ring.

  ## Quick reference

      (query/recent {:where {:level :error} :since \"1h\"})
      (query/lifecycle \"r-42\")
      (query/errors-since \"1h\")

  ## Filter-map shape

      {:where    {field val-or-set-or-tuple, …}   ; implicit AND
       :since    \"10m\" | \"2026-05-13T11:00:00Z\"
       :until    \"1h\"  | \"2026-05-13T12:00:00Z\"
       :limit    50                               ; default 100, any positive int
       :order-by [:inst :desc]                    ; default
       :group-by :error-class                     ; store-dependent
       :count?   false}                           ; store-dependent

  ## Field references

  - Bare keyword: built-in column (`:level :ns :id :msg :request-id
    :user-xid :tenant :host :error-class :error-msg :error-trace :inst :v`)
  - Vector path: `[:data k …]` or `[:ctx k …]` — arbitrary nesting

  ## Tuple operators

  Comparison: `:= :!= :> :< :>= :<=`
  Membership: `:in` (set value is also accepted bare)
  Text:       `:contains :icontains :starts-with :ends-with`
  Pattern:    `:matches` (Java regex)
  Presence:   `:exists? :absent?` (no second arg)"
  (:require
   [clojure.set]
   [synthigy.log.store :as store]))

;;; ============================================================================
;;; Spec — the filter-map vocabulary
;;; ============================================================================

(def allowed-top-level-keys
  "The seven keys the filter map accepts. Other top-level keys are user
   errors and `validate-filter-map` rejects them."
  #{:where :since :until :limit :order-by :group-by :count?})

(def comparison-ops
  #{:= :!= :> :< :>= :<=})

(def membership-ops
  #{:in})

(def text-ops
  "Take a String value."
  #{:contains :icontains :starts-with :ends-with})

(def regex-ops
  "Take a `java.util.regex.Pattern` value."
  #{:matches})

(def presence-ops
  "No second arg in the tuple — `[:exists?]` / `[:absent?]`."
  #{:exists? :absent?})

(def array-ops
  "Membership against an ARRAY-valued field (currently only `:topics`):
   `[:has :system]` ⇒ the field's set contains the given value. The mirror of
   `:in` — `:in` asks 'is the field-value in this set', `:has` asks 'is this
   value in the field's set'."
  #{:has})

(def all-ops
  (clojure.set/union comparison-ops membership-ops text-ops regex-ops
                     presence-ops array-ops))

(def built-in-columns
  "Top-level wire-schema fields addressable as bare keywords in `:where`.
   `:topics` is array-valued (use the `:has` op); the rest are scalars."
  #{:level :ns :id :msg :request-id :user-xid :tenant
    :host :error-class :error-msg :error-trace :inst :v :topics})

(def default-limit 100)

;;; ============================================================================
;;; Validation
;;; ============================================================================

(defn- field-ref? [x]
  (or (and (keyword? x) (contains? built-in-columns x))
      (and (vector? x)
           (>= (count x) 2)
           (contains? #{:data :ctx} (first x))
           (every? keyword? x))))

(defn- tuple? [v]
  (and (vector? v) (keyword? (first v)) (contains? all-ops (first v))))

(defn- validate-tuple [field tuple]
  (let [[op arg :as t] tuple
        n-args (dec (count t))]
    (cond
      (contains? presence-ops op)
      (when-not (= 0 n-args)
        (throw (ex-info (str "Operator " op " takes no arguments: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? array-ops op)
      (when-not (and (= 1 n-args) (or (keyword? arg) (string? arg)))
        (throw (ex-info (str "Operator " op " requires one keyword/string argument: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? membership-ops op)
      (when-not (set? arg)
        (throw (ex-info (str "Operator " op " requires a set value: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? regex-ops op)
      (when-not (instance? java.util.regex.Pattern arg)
        (throw (ex-info (str "Operator " op " requires a regex pattern: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? text-ops op)
      (when-not (string? arg)
        (throw (ex-info (str "Operator " op " requires a String: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? comparison-ops op)
      (when (nil? arg)
        (throw (ex-info (str "Operator " op " requires a non-nil argument: " field " " tuple)
                        {:field field :tuple tuple :op op}))))))

(defn- validate-where [where]
  (when (some? where)
    (when-not (map? where)
      (throw (ex-info ":where must be a map" {:where where})))
    (doseq [[field v] where]
      (when-not (field-ref? field)
        (throw (ex-info (str "Unknown field reference: " (pr-str field)
                             ". Use a built-in column keyword "
                             "(" built-in-columns ") "
                             "or a [:data …] / [:ctx …] path vector.")
                        {:field field :where where})))
      (cond
        (tuple? v)  (validate-tuple field v)
        (set? v)    nil          ; bare set = [:in …]
        :else       nil))))      ; bare value = equality

(defn- validate-since-until [k v]
  (when (some? v)
    (when-not (string? v)
      (throw (ex-info (str k " must be a duration string (e.g. \"10m\") or ISO instant")
                      {k v})))))

(defn- validate-order-by [order-by]
  (when (some? order-by)
    (when-not (and (vector? order-by) (= 2 (count order-by))
                   (#{:asc :desc} (second order-by)))
      (throw (ex-info ":order-by must be [field :asc] or [field :desc]"
                      {:order-by order-by})))
    (when-not (or (and (keyword? (first order-by))
                       (contains? built-in-columns (first order-by)))
                  (and (vector? (first order-by))
                       (contains? #{:data :ctx} (ffirst (vector (first order-by))))))
      (throw (ex-info (str ":order-by field must be a built-in column or "
                           "a [:data …] / [:ctx …] path")
                      {:order-by order-by})))))

(defn- validate-limit [limit]
  (when (some? limit)
    (when-not (and (integer? limit) (pos? limit))
      (throw (ex-info ":limit must be a positive integer"
                      {:limit limit})))))

(defn validate-filter-map
  "Throw `ex-info` if `filter-map` violates the spec. Returns nil on success."
  [filter-map]
  (when-not (map? filter-map)
    (throw (ex-info "Filter must be a map" {:got filter-map})))
  (let [unknown (clojure.set/difference (set (keys filter-map)) allowed-top-level-keys)]
    (when (seq unknown)
      (throw (ex-info (str "Unknown top-level filter keys: " unknown
                           ". Allowed: " allowed-top-level-keys)
                      {:unknown unknown :allowed allowed-top-level-keys}))))
  (validate-where    (:where filter-map))
  (validate-since-until :since (:since filter-map))
  (validate-since-until :until (:until filter-map))
  (validate-limit    (:limit filter-map))
  (validate-order-by (:order-by filter-map))
  (when (and (:count? filter-map) (not (boolean? (:count? filter-map))))
    (throw (ex-info ":count? must be boolean" {:count? (:count? filter-map)})))
  nil)

;;; ============================================================================
;;; Public API — query via *log-store*
;;; ============================================================================

(defn- require-store []
  (or @#'store/*log-store*
      (throw (ex-info "No log store bound — call synthigy.log/install! first"
                      {:cause :no-log-store}))))

(defn query
  "Execute a filter-map against the currently bound `*log-store*`. Validates
   first, then delegates to the store's `search`. Returns:
   - a vector of wire-schema-v1 rows for regular queries
   - a Long when `:count?` is set
   - a vector of group buckets when `:group-by` is set"
  [filter-map]
  (validate-filter-map filter-map)
  (store/search (require-store) filter-map))

(defn current-store-backend
  "Return a keyword identifying the bound store's backend, or nil if none
   bound. Useful for diagnostics — cockpit, admin endpoints. Reads
   `:backend` off the store's `health` snapshot."
  []
  (when-let [s @#'store/*log-store*]
    (try (:backend (store/health s)) (catch Throwable _ nil))))

;;; ============================================================================
;;; Sugar — thin shortcuts to common filter shapes
;;; ============================================================================

(defn lifecycle
  "All signals for one request, oldest-first. Sugar for
   `{:where {:request-id rid} :order-by [:inst :asc]}`."
  [request-id]
  (query {:where {:request-id request-id} :order-by [:inst :asc]}))

(defn errors-since
  "Error+fatal signals from `since` until now. `since` is a duration string
   (e.g. `\"10m\"`, `\"1h\"`) or ISO instant."
  ([since] (errors-since since default-limit))
  ([since limit]
   (query {:where {:level #{:error :fatal}}
           :since since
           :limit limit})))
