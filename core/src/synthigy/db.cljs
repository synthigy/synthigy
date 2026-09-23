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
  "Database abstraction protocol and global state (ClojureScript-specific).")

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
  (aggregate-entity
    [this entity-id args selection]
    "Takes dataset entity id, arguments and selection to return aggregated values
    for given args and selection. Possible fields in selection are:
    * count
    * max
    * min
    * avg")
  (aggregate-entity-tree
    [this entity-id on args selection]
    "Takes dataset entity id 'on' recursion with arguments
    and selection to return aggregated values for given args and selection.
    Possible fields in selection are:
    * count
    * max
    * min
    * avg")
  (delete-entity
    [this entity-id data]
    "Function takes dataset entity id and data to delete entities from
    from DB"))

;;; ============================================================================
;;; Database Records
;;; ============================================================================

(defrecord Postgres [host port user db password max-connections datasource])
