;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
;;; Login Page Store
;;; ============================================================================

(defprotocol LoginPageStore
  "Versioned zip bundles served as custom OAuth login pages."
  (put-login-page [db name zip digest uploaded-by]
    "Store zip as the next version of name, inactive; returns that version.")
  (login-pages [db]
    "Every stored version without its zip, newest first.")
  (login-page-zip [db name version])
  (active-login-pages [db]
    "{name {:version :digest}} for every active bundle.")
  (activate-login-page [db name version]
    "Make version the only active one of name; nil version deactivates name.")
  (delete-login-page [db name version]))

;;; ============================================================================
;;; Database Error Translation
;;; ============================================================================

(defprotocol Translator
  (translate-db-exception [db ^java.sql.SQLException e]
    "Translate a native SQL exception into an ex-info with canonical :code and optional :details, or nil if unrecognized; must be pure."))

(extend-protocol Translator
  SQLite
  (translate-db-exception [_ _] nil)
  Cockroach
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
