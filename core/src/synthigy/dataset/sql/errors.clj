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

(ns synthigy.dataset.sql.errors
  "Deploy failures phrased in model terms, shared by every SQL backend."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db :refer [*db*]]
   [synthigy.db.sql :refer [execute-one!]]))

(def drift-hint "The database was changed outside Synthigy, or an earlier deploy was interrupted.")
(def conversion-hint "Fix or clear the values that don't fit the new type, or keep the old type.")
(def retry-hint "Other work is holding the table; retry when it is quieter.")

(defn translate
  [e]
  (when (instance? java.sql.SQLException e)
    (some-> *db* (db/translate-db-exception e))))

(defn attribute-label
  [entity column]
  (or (some (fn [{n :name}] (when (= column (naming/normalize-name n)) n))
            (:attributes entity))
      column))

(defn quote-ident
  [s]
  (str \" (str/replace s "\"" "\"\"") \"))

(defn duplicated-values
  "Count of values shared by more than one row, plus one sample for a single column."
  [conn table columns]
  (let [single? (= 1 (count columns))
        cols    (str/join ", " (map quote-ident columns))]
    (try
      (execute-one!
       (or conn (:datasource *db*))
       [(format "select count(*) as n%s from (select %s from %s where %s group by %s having count(*) > 1) d"
                (if single? ", min(k) as sample" "")
                (if single? (str (quote-ident (first columns)) " as k") cols)
                (quote-ident table)
                (str/join " and " (map #(str (quote-ident %) " is not null") columns))
                cols)])
      (catch Throwable _ nil))))

(defn ddl-column
  [statement]
  (some->> statement (re-find #"(?i)alter column \"?([^\"\s]+)\"? type") second))

(defn target-label
  [entity-name attrs]
  (cond
    (= 1 (count attrs)) (str entity-name "." (first attrs))
    (seq attrs)         (str entity-name " (" (str/join ", " attrs) ")")
    :else               entity-name))

(defn deploy-ddl-error
  "Ex-info for a failed deploy DDL statement, phrased in model terms when the DB error is recognized."
  [message data entity statement e & [conn]]
  (if-let [translated (translate e)]
    (let [{:keys [code details hint retryable]} (ex-data translated)
          n       (:name entity)
          table   (:entity details)
          reason  (ex-message translated)
          columns (or (not-empty (:attributes details)) (some-> (ddl-column statement) vector))
          attrs   (mapv #(attribute-label entity %) columns)
          target  (target-label n attrs)
          dupes   (when (and (= "UNIQUE_VIOLATION" code) table (seq columns))
                    (duplicated-values conn table columns))
          values  (or (not-empty (:values details))
                      (some-> (:sample dupes) str vector))
          example (cond
                    (empty? values) nil
                    (and (< 1 (count values)) (= (count attrs) (count values)))
                    (str " (e.g. " (str/join ", " (map #(str %1 " = " %2) attrs values)) ")")
                    :else (str " (e.g. " (str/join ", " values) ")"))
          [msg default-hint]
          (case code
            "UNIQUE_VIOLATION"
            [(str "Can't make " target " unique: "
                  (let [c (:n dupes)]
                    (cond (nil? c) "some values are"
                          (= 1 c)  "1 value is"
                          :else    (str c " values are")))
                  " shared by more than one record" example)
             "Remove or merge the duplicate records, or make the attribute unique together with another attribute."]

            "NOT_NULL_VIOLATION"
            [(str "Existing " target " records have no value, but the new schema requires one")
             "Fill in the missing values first, or keep the attribute optional."]

            ("INVALID_VALUE" "TYPE_MISMATCH")
            [(str "Can't convert existing " target " values: " reason)
             conversion-hint]

            "FK_VIOLATION"
            [(str "Can't link " n ": existing records reference missing records" example)
             "Remove the dangling references first."]

            ("SCHEMA_CONFLICT" "SCHEMA_DRIFT" "DEPENDENT_OBJECTS")
            [(str "Database schema for " n " doesn't match the model: " reason)
             drift-hint]

            ("LOCKED" "TIMEOUT")
            [(str "Deploy of " n " was blocked: " reason)
             retry-hint]

            "GUARD_VIOLATION"
            [reason nil]

            [(str message ": " reason) nil])]
      (ex-info msg
               (cond-> (assoc data
                              :code code
                              :details (cond-> (assoc details :table table)
                                         n            (assoc :entity n)
                                         (seq attrs)  (assoc :attributes attrs)
                                         (seq values) (assoc :values values)))
                 (or hint default-hint) (assoc :hint (or hint default-hint))
                 retryable              (assoc :retryable true))
               e))
    (ex-info message data e)))

(defn invalid-conversion-error
  [entity attribute to-type failures sample]
  (ex-info (format "Can't convert existing %s values to %s: %d %s can't be converted (e.g. %s)"
                   (target-label (:name entity) [(:name attribute)])
                   to-type failures (if (= 1 failures) "value" "values") (pr-str sample))
           {:code    "INVALID_VALUE"
            :hint    conversion-hint
            :details {:entity     (:name entity)
                      :attributes [(:name attribute)]
                      :values     [(str sample)]
                      :to-type    to-type}}))

(defn relation-label
  [{:keys [from to to-label]}]
  (str (:name (or from to)) " → " (:name to) " relation"
       (when (seq to-label) (str " (" to-label ")"))))

(defn relation-part
  [operation]
  (case operation
    (:rename-to-column :rename-from-column :add-recursive-column
     :add-new-recursive-column :rename-recursive-column) "column"
    (:rename-from-index :rename-to-index :create-from-index :create-to-index) "index"
    "table"))

(defn relation-ddl-error
  "Ex-info for a failed relation DDL statement, phrased in model terms when the DB error is recognized."
  [relation message data e]
  (if-let [translated (translate e)]
    (let [{:keys [code details hint retryable]} (ex-data translated)
          operation (:operation data)
          rename?   (str/starts-with? (name (or operation :create)) "rename")
          verb      (if rename? "rename" "create")
          label     (relation-label relation)
          part      (relation-part operation)
          a-part    (str (if (= "index" part) "an " "a ") part)
          reason    (ex-message translated)
          [msg default-hint]
          (case code
            "SCHEMA_CONFLICT"
            [(str "Can't " verb " the " label ": the database already has " a-part
                  (if rename? " under the new name" " with that name"))
             drift-hint]

            "SCHEMA_DRIFT"
            [(str "Can't " verb " the " label ": its " part " is missing from the database")
             drift-hint]

            "DEPENDENT_OBJECTS"
            [(str "Can't " verb " the " label ": other database objects depend on its " part)
             drift-hint]

            ("LOCKED" "TIMEOUT")
            [(str "Deploy of the " label " was blocked: " reason)
             retry-hint]

            [(str "Can't " verb " the " label ": " reason) nil])]
      (ex-info msg
               (cond-> (assoc data
                              :code code
                              :details (-> details
                                           (dissoc :entity :attributes)
                                           (assoc :table (:entity details)
                                                  :relation label
                                                  :reason reason)))
                 (or hint default-hint) (assoc :hint (or hint default-hint))
                 retryable              (assoc :retryable true))
               e))
    (ex-info message data e)))
