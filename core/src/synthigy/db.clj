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

(ns synthigy.db
  "Database abstraction protocols, records and global state.")

;;; ============================================================================
;;; Global State
;;; ============================================================================

(defonce ^:dynamic *db* nil)

;;; ============================================================================
;;; Database Abstraction Protocol
;;; ============================================================================

(defprotocol ModelQueryProtocol
  (sync-entity
    [this entity-id data]
    "Synchronize entity records with input data, removing relations absent from input.")
  (stack-entity
    [this entity-id data]
    "Stack input data on top of current DB state.")
  (slice-entity
    [this entity-id args selection]
    "Delete relations between entities based on input data.")
  (get-entity
    [this entity-id args selection]
    "Return a single record matching args, shaped by selection.")
  (get-entity-tree
    [this entity-id root on selection]
    "Return records as a tree built from root by recursing 'on'.")
  (search-entity
    [this entity-id args selection]
    "Return all records matching args, shaped by selection.")
  (search-entity-tree
    [this entity-id on args selection]
    "Search records with recursive 'on' traversal, shaped by selection.")
  (purge-entity
    [this entity-id args selection]
    "Delete all records matching args and return deleted data per selection.")
  (delete-entity
    [this entity-id data]
    "Delete entity records identified by data."))

;;; ============================================================================
;;; Database Records
;;; ============================================================================

(defrecord Postgres  [host port user db password max-connections datasource])
(defrecord SQLite    [path datasource])
(defrecord Cockroach [host port user db password max-connections datasource])

;;; ============================================================================
;;; Database Error Translation
;;; ============================================================================

(defprotocol Translator
  (translate-db-exception [db ^java.sql.SQLException e]
    "Translate a native SQL exception into an ex-info with canonical :code and optional :details, or nil if unrecognized; must be pure."))

(extend-protocol Translator
  Postgres
  (translate-db-exception [_ _] nil)
  SQLite
  (translate-db-exception [_ _] nil))

;;; ============================================================================
;;; Dialect
;;; ============================================================================

(defprotocol Dialect
  "Per-backend SQL fragments and JDBC value codecs; shared code never branches on backend class."
  (json-param [db json-str]
    "JDBC parameter value for a JSON column, from a JSON string.")
  (json-column [db v]
    "JSON string from a JSON column value as JDBC returned it.")
  (table-exists? [db table])
  (column-exists? [db table column])
  (ddl [db]
    "DDL fragment map: :serial-pk, :json, :now.")
  (json-text [db expr]
    "Expression yielding JSON `expr` as text.")
  (json-get-text [db expr k]
    "Expression yielding key `k` of JSON `expr` as text.")
  (json-remove [db expr k]
    "Expression yielding JSON `expr` without key `k`.")
  (cast-placeholder [db type]
    "Bind placeholder cast to SQL type `type`.")
  (template-sql [db raw-sql]
    "Adapt Postgres-dialect template SQL for this backend."))
