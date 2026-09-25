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

(ns synthigy.dataset.codegen
  "XSQL program → language-neutral codegen IR (what op:\"describe\" returns)."
  (:require [synthigy.xsql.program :as prog]
            [synthigy.xsql.sql-params :as sp]
            [clojure.string :as str]))

(def ^:private read-verbs #{"search" "get" "slice" "purge"})

(def ^:private op-start-re
  "Matches an operation-start `@verb` line; header directives excluded."
  #"@(?:search|get|sql-template|slice|purge|sync|stack|delete|batch)\b")

(defn returns->result
  "Synthesizes a result tree from a sql-template `@returns` spec."
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

(defn derive-result
  "Derives the typed result tree for a selection against the snake schema."
  [schema entity-name selection]
  (let [ent (get-in schema [:entities entity-name])]
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
                 ;; _count/_agg must be tested BEFORE the relation branch —
                 ;; their
                 ;; wire shape mimics a relation and would be mistyped
                 ;; otherwise.
                 (= kname "_count")
                 (conj acc {:key "_count" :kind "map" :value "int" :optional true})
                 (= kname "_agg")
                 (conj acc {:key "_agg" :kind "map" :value "unknown" :optional true})

                 (and (vector? v) (map? (first v)))
                 (let [{:keys [selections alias]} (first v)
                       rel (get-in ent [:relations kname])]
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
                 :else
                 (if (= kname "xid") acc
                     (let [attr (get-in ent [:attributes kname])]
                       (when (nil? attr)
                         (throw (ex-info (str "describe: '" kname "' is not an attribute on '" entity-name "'")
                                         {:code "SCHEMA_UNKNOWN_ATTRIBUTE" :entity entity-name :attribute kname})))
                       (conj acc (cond-> {:key kname :type (:type attr)}
                                   (= false (:nullable attr)) (assoc :nullable false)
                                   (:enum attr)               (assoc :enum (:enum attr)))))))))
           [{:key "xid" :type "string" :nullable false}]
           selection)
          (filterv some?))}))

(defn ir-params
  "Typed params scanned from one op's source segment."
  [segment]
  (mapv (fn [{:keys [name raw-type array? default optional? type-args]}]
          (cond-> {:name name :type raw-type :array (boolean array?)}
            (or (some? default) optional?) (assoc :optional true)
            type-args       (assoc :values type-args)))
        (sp/scan-placeholders segment)))

(defn op-segments
  "Splits a program into per-operation source segments."
  [source]
  (->> (str/split source (re-pattern (str "(?m)^(?=" op-start-re ")")))
       (map str/trim)
       (filterv #(re-find (re-pattern (str "^" op-start-re)) %))))

(defn op-body
  "Strips the @-header from an op segment, leaving the rooted XSQL body."
  [segment]
  (let [lines (str/split-lines segment)
        start (loop [i 0, in-desc? false]
                (when (< i (count lines))
                  (let [l (nth lines i)]
                    (cond
                      in-desc?
                      (recur (inc i) (not (str/includes? l "\"")))
                      (and (str/starts-with? l "@description")
                           (odd? (count (re-seq #"\"" l))))
                      (recur (inc i) true)
                      (and (seq l) (not (str/starts-with? l "@"))
                           (not (str/starts-with? l " ")))
                      i
                      :else (recur (inc i) false)))))]
    (if start (str/trim (str/join "\n" (drop start lines))) "")))

(defn file-ops
  [{:keys [path source]}]
  (mapv #(assoc %1 :path path :segment %2)
        (filterv #(or (:op %) (:batch %)) (prog/compile source nil))
        (op-segments source)))

(defn where
  [paths]
  (let [ps (map #(or % "the program") paths)]
    (if (apply = ps)
      (str (count ps) " times in " (first ps))
      (str "in " (str/join " and " (distinct ps))))))

(defn identity-of
  [op]
  (if (:batch op) (str "@batch " (:name op)) (prog/op-identity op)))

(defn check-program!
  "Refuse what codegen can't emit: unplaceable templates, duplicate identities, unresolvable @batch members."
  [ops]
  (when-let [orphans (seq (map :name (prog/missing-namespace-ops ops)))]
    (throw (ex-info (str "@sql-template with no root entity needs @namespace: "
                         (str/join ", " orphans))
                    {:code "NAMESPACE_REQUIRED" :ops (vec orphans)})))
  (when-let [dups (seq (filter #(> (count (val %)) 1) (group-by identity-of ops)))]
    (throw (ex-info (str/join "; " (for [[id os] dups]
                                     (str "duplicate operation '" id "' — declared "
                                          (where (map :path os)))))
                    {:code "DUPLICATE_OPERATION"
                     :duplicates (vec (for [[id os] dups] {:identity id :paths (mapv :path os)}))})))
  (doseq [[_ file] (group-by :path ops)
          b (filter :batch file)
          m (:members b)
          :let [{:keys [missing ambiguous]} (prog/resolve-batch-member file m)]
          :when (or missing ambiguous)]
    (throw (ex-info (str "@batch '" (:name b) "' in " (or (:path b) "the program") ": '" m "' "
                         (if missing
                           "is not declared in the same file — a @batch only runs ops from its own file"
                           (str "is ambiguous (" (str/join ", " (map prog/op-identity ambiguous)) ") — qualify it")))
                    {:code "BATCH_MEMBER_UNRESOLVED" :batch (:name b) :member m :path (:path b)}))))

(defn describe
  "XSQL program → codegen IR `{:operations [...]}`. `sources` is one program
   string or `[{:path :source}]`, one per file — each file is parsed on its own,
   so its buffer-level @namespace stays in it."
  [schema sources _params]
  (let [files (if (string? sources) [{:source sources}] sources)
        ops   (into [] (mapcat file-ops) files)]
    (check-program! ops)
    {:operations
     (mapv (fn [op]
             (if (:batch op)
               (let [file (filter #(= (:path op) (:path %)) ops)]
                 {:name (:name op) :batch true
                  :members (mapv (fn [m] (prog/op-identity (:op (prog/resolve-batch-member file m))))
                                 (:members op))})
               ;; scan params from the body only — @returns' `?` (nullable)
               ;; in the header would alias the ?param sigil
               (let [body (op-body (:segment op))]
                (cond-> {:name   (:name op)
                        :op     (:op op)
                        :entity (:entity op)
                        :params (ir-params body)
                        :source body}
                 (:description op) (assoc :description (:description op))
                 (:namespace op) (assoc :namespace (:namespace op))
                 (:watch op)     (assoc :watch (:watch op))
                 (read-verbs (:op op))
                 (assoc :result (derive-result schema (:entity op) (:selections op)))
                 (and (= "sql-template" (:op op)) (:returns op))
                 (assoc :result (returns->result (:returns op)))))))
           ops)}))
