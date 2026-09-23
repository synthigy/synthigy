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

(ns synthigy.dataset.sql.protocol
  "Core protocols for database abstraction, extended over database record types.")

;;; ============================================================================
;;; Protocol 1: TypeCodec
;;; ============================================================================

(defprotocol TypeCodec
  "Encode/decode values to/from database-specific types."
  (encode [db type value]
    "Encode Clojure value to database-specific representation for the ERD model type.")
  (decode [db type value]
    "Decode database value to Clojure representation for the ERD model type."))

;;; ============================================================================
;;; Protocol 2: SQLDialect
;;; ============================================================================

(defprotocol SQLDialect
  "Generate database-specific SQL syntax."
  (like-operator [db case-sensitive?]
    "Return the LIKE operator for the requested case sensitivity.")
  (limit-offset-clause [db limit offset]
    "Generate LIMIT/OFFSET clause for pagination.")
  (placeholder-for-type [db field-type]
    "Generate placeholder for a parameterized value with optional type cast.")
  (excluded-ref [db column]
    "Generate reference to the excluded row in an UPSERT.")
  (max-bind-params [db]
    "Hard cap on bound parameters per prepared statement, used to size bulk-write chunks."))

;;; ============================================================================
;;; Protocol 3: SchemaManager
;;; ============================================================================

(defprotocol SchemaManager
  "Query and manage database schema."
  (get-tables [db]
    "List all non-system table names.")
  (get-columns [db table]
    "Return column info maps (:name :type :nullable :default) for a table.")
  (get-enums [db]
    "Return enum info maps (:name :values), or nil if unsupported.")
  (table-exists? [db table]
    "Check if table exists.")
  (column-exists? [db table column]
    "Check if column exists in table.")
  (list-tables-like [db pattern]
    "List tables matching SQL LIKE pattern.")
  (drop-table! [db table]
    "Drop a single table with IF EXISTS + CASCADE; true if dropped.")
  (drop-tables-like! [db pattern]
    "Drop all tables matching SQL LIKE pattern; returns count dropped.")
  (truncate-table! [db table]
    "Remove all rows from a table, keeping its structure.")
  (list-types-like [db pattern]
    "List custom types matching SQL LIKE pattern.")
  (drop-type! [db type-name]
    "Drop a single custom type with IF EXISTS + CASCADE; true if dropped.")
  (drop-types-like! [db pattern]
    "Drop all custom types matching SQL LIKE pattern; returns count dropped."))
