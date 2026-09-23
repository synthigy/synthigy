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

(ns synthigy.db.sql
  "Database-agnostic JDBC execution layer over the JDBCBackend protocol."
  (:require
    [next.jdbc :as jdbc]
    [synthigy.db :as db :refer [*db*]]
    [synthigy.json :as json]))

;;; ============================================================================
;;; JDBCBackend Protocol
;;; ============================================================================

(defprotocol JDBCBackend
  "Database-specific JDBC result set handling."
  (jdbc-options [db return-type]
    "Return next.jdbc options map for query execution given :edn or :raw return-type."))

;;; ============================================================================
;;; Shared Utilities
;;; ============================================================================

(defn connectable?
  "Return true if x is a JDBC connectable."
  [x]
  (or (instance? javax.sql.DataSource x)
      (instance? java.sql.Connection x)
      (and (map? x) (contains? x :connection))))

(defn normalize-statement
  "Convert map parameters in a JDBC statement to JSON strings."
  [[q & params :as statement]]
  (if (not-empty params)
    (concat [q] (map #(if (map? %) (db/json-param *db* (json/->json %)) %) params))
    statement))

(defn get-label-fn
  "Return the column label transformation function for the given return type."
  [return-type]
  (get-in (jdbc-options *db* return-type) [:label-fn] identity))

;;; ============================================================================
;;; Prepared Statements
;;; ============================================================================

(defn prepare
  "Create a prepared statement using *db* options."
  [connectable statement]
  (jdbc/prepare connectable statement (jdbc-options *db* :raw)))

;;; ============================================================================
;;; Execute Functions
;;; ============================================================================

(defn execute!
  "Execute JDBC statement using *db* or explicit connectable."
  ([statement]
   (execute! (:datasource *db*) statement nil))
  ([connectable-or-statement statement-or-return-type]
   (if (connectable? connectable-or-statement)
     (execute! connectable-or-statement statement-or-return-type nil)
     (execute! (:datasource *db*) connectable-or-statement statement-or-return-type)))
  ([connectable statement return-type]
   (jdbc/execute!
     connectable
     (normalize-statement statement)
     (jdbc-options *db* (or return-type :raw)))))

(defn execute-one!
  "Execute JDBC statement returning a single result using *db* or explicit
   connectable."
  ([statement]
   (execute-one! (:datasource *db*) statement nil))
  ([connectable-or-statement statement-or-return-type]
   (if (connectable? connectable-or-statement)
     (execute-one! connectable-or-statement statement-or-return-type nil)
     (execute-one! (:datasource *db*) connectable-or-statement statement-or-return-type)))
  ([connectable statement return-type]
   (jdbc/execute-one!
     connectable
     (normalize-statement statement)
     (jdbc-options *db* (or return-type :raw)))))

(defn execute-batch!
  "Execute batch JDBC statement or PreparedStatement using *db* or explicit
   connectable."
  ([statement]
   (execute-batch! statement nil))
  ([statement-or-connectable param-sets-or-statement-or-return-type]
   (cond
     (instance? java.sql.PreparedStatement statement-or-connectable)
     (execute-batch! statement-or-connectable param-sets-or-statement-or-return-type nil)

     (connectable? statement-or-connectable)
     (execute-batch! statement-or-connectable param-sets-or-statement-or-return-type nil)

     :else
     (let [[sql & param-groups] (normalize-statement statement-or-connectable)]
       (jdbc/execute-batch!
         (:datasource *db*)
         sql
         param-groups
         (jdbc-options *db* (or param-sets-or-statement-or-return-type :raw))))))
  ([connectable-or-prepared statement-or-param-sets return-type]
   (cond
     (instance? java.sql.PreparedStatement connectable-or-prepared)
     (jdbc/execute-batch!
       connectable-or-prepared
       statement-or-param-sets
       (jdbc-options *db* (or return-type :raw)))

     (connectable? connectable-or-prepared)
     (let [[sql & param-groups] (normalize-statement statement-or-param-sets)]
       (jdbc/execute-batch!
         connectable-or-prepared
         sql
         param-groups
         (jdbc-options *db* (or return-type :raw))))

     :else
     (throw (ex-info "Invalid execute-batch! arguments"
                     {:args [connectable-or-prepared statement-or-param-sets return-type]})))))
