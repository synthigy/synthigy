(ns synthigy.db
  "Database abstraction protocol and global state.

  This namespace provides the core database protocols and records.
  Backend selection is now done via deps.edn aliases (:postgres or :sqlite)
  rather than runtime environment variables.

  ## Usage

  Run with backend alias:
    clj -M:postgres:dev   ;; PostgreSQL
    clj -M:sqlite:dev     ;; SQLite

  Then:
    (require '[synthigy.db :as db])
    db/*db*  ;; Bound after lifecycle/start! :synthigy/database")

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
    "Sync entity takes dataset entity id and data and
    synchronizes DB with current state. This includes
    inserting/updating new records and relations as
    well as removing relations that were previously linked
    with input data and currently are not")
  (stack-entity
    [this entity-id data]
    "Stack takes dataset entity id and data to stack
    input data on top of current DB state")
  (slice-entity
    [this entity-id args selection]
    "Slice takes dataset entity id and data to slice
    current DB state based on input data effectively deleting
    relations between entities")
  (get-entity
    [this entity-id args selection]
    "Takes dataset entity id, arguments to pinpoint target row
    and selection that specifies which attributes and relations
    should be returned")
  (get-entity-tree
    [this entity-id root on selection]
    "Takes dataset entity id, root record and constructs tree based 'on'.
    Selection that specifies which attributes and relations
    should be returned")
  (search-entity
    [this entity-id args selection]
    "Takes dataset entity id, arguments to pinpoint target rows
    and selection that specifies which attributes and relations
    should be returned")
  (search-entity-tree
    [this entity-id on args selection]
    "Takes dataset entity id, arguments to pinpoint target rows
    based 'on' recursion and selection that specifies which attributes and relations
    should be returned")
  (purge-entity
    [this entity-id args selection]
    "Find all records that match arguments, delete found records
    and return deleted information based on selection input")
  (delete-entity
    [this entity-id data]
    "Function takes dataset entity id and data to delete entities from
    from DB"))

;;; ============================================================================
;;; Database Records
;;; ============================================================================

(defrecord Postgres  [host port user db password max-connections datasource])
(defrecord SQLite    [path datasource])
(defrecord Cockroach [host port user db password max-connections datasource])

;;; ============================================================================
;;; Database Error Translation
;;; ============================================================================
;;
;; Backend-neutral vocabulary for native DB exceptions surfaced through `/data`.
;; A backend's `translate-db-exception` method maps a `java.sql.SQLException`
;; into an `ex-info` whose `ex-data` carries one of the canonical `:code`
;; strings below plus an optional `:details` map. The request handler turns
;; that into the wire response's `error.code` / `error.details`. Backends
;; that don't recognize an exception return `nil`; the handler then falls
;; back to `INTERNAL_ERROR` (with the original stack logged).
;;
;; Canonical codes:
;;
;;   "UNIQUE_VIOLATION"    duplicate value on a unique constraint
;;     :details {:entity name :attrs [name ...] :constraint name?}
;;
;;   "FK_VIOLATION"        FK reference to missing parent row
;;     :details {:entity name? :attr name? :target_entity name?}
;;
;;   "NOT_NULL_VIOLATION"  required attribute is null
;;     :details {:entity name :attr name}
;;
;;   "CHECK_VIOLATION"     CHECK constraint failed
;;     :details {:entity name? :constraint name?}
;;
;;   "TYPE_CAST_FAILURE"   value cannot be converted to column type
;;     :details {:entity name? :attr name? :given any?}
;;
;;   "TIMEOUT"             statement timed out / canceled
;;     :details {}
;;
;; `:details` fields are best-effort: each backend populates what it can
;; extract from the native exception. Clients should treat any field as
;; optional. The wire response omits `:details` entirely when the backend
;; produced an empty map — clients should treat absence and `{}` as
;; equivalent.
;;
;; TODO — extend the contract-explanation shape (`:rule` + `:entity` +
;; `:attributes`) to non-DB error codes for client-side parser uniformity.
;; Today these still emit code + message only:
;;
;;   UNKNOWN_ENTITY     → :details {:rule "schema_name" :entity name}
;;   UNKNOWN_RELATION   → :details {:rule "schema_name" :entity :attributes}
;;   UNKNOWN_OP         → :details {:rule "operation" :op name}
;;   FORBIDDEN          → :details {:rule "scope" :scope name}
;;   FORBIDDEN_OP       → :details {:rule "policy"}
;;   MISSING_ON         → :details {:rule "operation_arg" :argument "on"}
;;   MISSING_ROOT       → :details {:rule "operation_arg" :argument "root"}
;;   XSQL_PARSE_ERROR   → already carries :diagnostics; could also gain :rule
;;
;; Touches the throw-sites in synthigy.dataset.sql.query, .template, and the
;; auth/scope guards in synthigy.server.data.

(defprotocol Translator
  (translate-db-exception [db ^java.sql.SQLException e]
    "Translate a native SQL exception into an ex-info with `:code` and
     optional `:details` in ex-data, or return nil if the backend doesn't
     recognize it. Pure: no logging or I/O."))

;; Default no-op for both backend records. Specific backends override this
;; in their own namespace via `extend-type` (see e.g. synthigy.db.sqlite).
(extend-protocol Translator
  Postgres
  (translate-db-exception [_ _] nil)
  SQLite
  (translate-db-exception [_ _] nil))
