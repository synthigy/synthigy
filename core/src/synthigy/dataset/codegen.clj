(ns synthigy.dataset.codegen
  "XSQL program → language-neutral codegen IR.

   The server parses XSQL and derives result types ONCE (it owns the grammar
   AND the IAM-filtered schema); per-language emitters consume the IR JSON and
   just render syntax — they never parse XSQL. This is what `op:\"describe\"`
   returns.

   The IR is language-neutral: no TS/Go names, no casing decisions, no struct
   naming. Path-named types, snake-vs-camel, `?:` vs `Option<T>` are all the
   emitter's job. Names here are snake_case (XSQL is snake-strict and the schema
   is snake-projected, so selection keys match schema keys directly — no bridge).

   Reads (search/get/slice/purge) get a selection-derived `:result` tree. Writes
   (sync/stack/delete) and sql-template carry name+verb+entity only — their input
   types are schema-derived and their result is engine-fixed / unsolved."
  (:require [synthigy.xsql.program :as prog]
            [synthigy.xsql.sql-params :as sp]
            [clojure.string :as str]))

(def ^:private read-verbs #{"search" "get" "slice" "purge"})

(def ^:private op-start-re
  "Lookahead splitter / matcher for an operation-start `@verb` line. Excludes
   header directives (@namespace/@watch/@returns/@description), which live WITHIN
   an op and must not start a new segment."
  #"@(?:search|get|sql-template|slice|purge|sync|stack|delete|batch)\b")

(defn- returns->result
  "Synthesize a result tree from a sql-template `@returns` spec
   ('a:int, b:float?, …'). `?` ⇒ nullable column. sql-templates carry no xid."
  [spec]
  {:kind "object"
   :fields (->> (str/split spec #",")
                (map str/trim)
                (remove str/blank?)
                (mapv (fn [col]
                        (if-let [[_ k t nul] (re-matches #"(\w+)\s*:\s*(\w+)\s*(\?)?" col)]
                          (cond-> {:key k :type t}
                            (not nul) (assoc :nullable false))
                          {:key col :type "unknown"}))))})

(defn- derive-result
  "Selection × snake schema → typed result tree:
     {:kind \"object\" :fields [{:key, :type, :nullable?, :enum?} |
                                {:key, :kind \"relation\", :cardinality, :optional, :fields}]}"
  [schema entity-name selection]
  (let [ent (get-in schema [:entities entity-name])]
    ;; HARD FAIL: an unresolvable entity means the schema we were handed is wrong
    ;; (e.g. the keywordized-vs-string-keyed landmine) — refuse to emit garbage
    ;; types instead of silently producing `type:"unknown"` / `cardinality:null`.
    (when (and entity-name (nil? ent))
      (throw (ex-info (str "describe: schema has no entity '" entity-name
                           "' — refusing to emit untyped result")
                      {:code "SCHEMA_UNKNOWN_ENTITY" :entity entity-name})))
    {:kind "object"
     :fields
     (->> (reduce-kv
           (fn [acc k v]
             (let [kname (name k)]
               (cond
                 ;; aggregates — `_count` is string→number, `_agg` is a deep,
                 ;; varied tree. We type them as honest maps rather than a precise
                 ;; nested type (not worth the machinery) — and critically, BEFORE
                 ;; the relation test, since their wire shape mimics a relation and
                 ;; would otherwise be mistyped as a relation-of-xids.
                 (= kname "_count")
                 (conj acc {:key "_count" :kind "map" :value "int" :optional true})
                 (= kname "_agg")
                 (conj acc {:key "_agg" :kind "map" :value "unknown" :optional true})

                 (and (vector? v) (map? (first v)))
                 ;; relation
                 (let [{:keys [selections alias]} (first v)
                       rel (get-in ent [:relations kname])]
                   ;; HARD FAIL: relation not in schema, or missing cardinality →
                   ;; the array-vs-singleton typing would be wrong. Don't guess.
                   (when (nil? rel)
                     (throw (ex-info (str "describe: '" kname "' is not a relation on '" entity-name "'")
                                     {:code "SCHEMA_UNKNOWN_RELATION" :entity entity-name :relation kname})))
                   (when (nil? (:cardinality rel))
                     (throw (ex-info (str "describe: relation '" entity-name "." kname "' has no cardinality")
                                     {:code "SCHEMA_MISSING_CARDINALITY" :entity entity-name :relation kname})))
                   (conj acc {:key         (or alias kname)
                              :kind        "relation"
                              :cardinality (:cardinality rel)
                              :optional    true
                              :fields      (:fields (derive-result schema (:to rel) selections))}))
                 ;; scalar — xid is prepended structurally, skip it in selection walk
                 :else
                 (if (= kname "xid") acc
                     (let [attr (get-in ent [:attributes kname])]
                       ;; HARD FAIL: selected field is neither attribute, relation,
                       ;; nor _count/_agg — it would emit `type:"unknown"`.
                       (when (nil? attr)
                         (throw (ex-info (str "describe: '" kname "' is not an attribute on '" entity-name "'")
                                         {:code "SCHEMA_UNKNOWN_ATTRIBUTE" :entity entity-name :attribute kname})))
                       (conj acc (cond-> {:key kname :type (:type attr)}
                                   (= false (:nullable attr)) (assoc :nullable false)
                                   (:enum attr)               (assoc :enum (:enum attr)))))))))
           [{:key "xid" :type "string" :nullable false}] ; structural at every level
           selection)
          (filterv some?))}))

(defn- ir-params
  "Typed params scanned from one op's source segment. `:default` ⇒ optional."
  [segment]
  (mapv (fn [{:keys [name raw-type array? default]}]
          (cond-> {:name name :type raw-type :array (boolean array?)}
            (some? default) (assoc :optional true)))
        (sp/scan-placeholders segment)))

(defn- op-segments
  "Split a program into per-operation source segments — one per op-start `@verb`
   line (NOT header directives). Drops any leading non-op content (a buffer-level
   `@namespace`, blank lines) so segments zip 1:1 with the non-anonymous ops from
   `prog/compile` (params scope to their own op)."
  [source]
  (->> (str/split source (re-pattern (str "(?m)^(?=" op-start-re ")")))
       (map str/trim)
       (filterv #(re-find (re-pattern (str "^" op-start-re)) %))))

(defn- op-body
  "The rooted XSQL body of an op segment — the @-header (`@verb name`,
   `@description` + its indented continuation) stripped, leaving the
   root-entity body the generated client embeds and sends."
  [segment]
  (let [lines (str/split-lines segment)
        start (->> (map-indexed vector lines)
                   (some (fn [[i l]]
                           ;; first flush-left, non-`@` line = the root entity
                           (when (and (seq l) (not (str/starts-with? l "@"))
                                      (not (str/starts-with? l " ")))
                             i))))]
    (if start (str/trim (str/join "\n" (drop start lines))) "")))

(defn describe
  "XSQL program source → codegen IR `{:operations [...]}`. Read ops get a typed
   `:result` tree + per-op `:params`; writes/sql-template carry name+op+entity;
   batches carry `:batch true :members` (member op names, composed by the emitter).
   `schema` is the IAM-filtered snake projection (daccess/schema)."
  [schema source _params]
  ;; Compile in TOOLING mode (nil params) — we derive TYPES, not values, so
  ;; missing required params must resolve to nil, not throw PARAM_MISSING.
  ;; Drop the anonymous leading chunk (`:op` nil) so ops zip 1:1 with op-segments.
  (let [ops  (filterv #(or (:op %) (:batch %)) (prog/compile source nil))
        segs (op-segments source)]
    {:operations
     (mapv (fn [op seg]
             (if (:batch op)
               {:name (:name op) :batch true :members (vec (:members op))}
               ;; Scan params from the BODY only — `@returns name:type?` in the
               ;; header carries `?` (nullable), which would alias the ?param sigil.
               (let [body (op-body seg)]
                (cond-> {:name   (:name op)
                        :op     (:op op)
                        :entity (:entity op)
                        :params (ir-params body)
                        :source body}
                 ;; identity + metadata, resolved by the canonical parser
                 (:description op) (assoc :description (:description op))
                 (:namespace op) (assoc :namespace (:namespace op))
                 (:watch op)     (assoc :watch (:watch op))
                 (read-verbs (:op op))
                 (assoc :result (derive-result schema (:entity op) (:selections op)))
                 (and (= "sql-template" (:op op)) (:returns op))
                 (assoc :result (returns->result (:returns op)))))))
           ops segs)}))
