(ns synthigy.data
  "Well-known system data entities.

  These are actual data records (not schema elements) with stable,
  well-known identifiers that persist across deployments.

  Uses compile-time multimethod registration for provider-agnostic ID resolution.

  Usage:
    ;; Get the ID via multimethod (resolves based on provider)
    (data :data/root-role)  ;; => UUID or XID

    ;; Or extract from record (checks both :euuid and :xid)
    (id/extract *ROOT*)  ;; => UUID or XID based on provider"
  (:require [synthigy.dataset.id :refer [defdata] :as id]))

;; =============================================================================
;; Well-Known Data ID Definitions
;; =============================================================================

(defdata :data/synthigy-user
  :euuid #uuid "c5a67922-351e-4ca3-95c2-fa52a7a3e2b5" :xid "RQb935cnLog5tiFsXgfMhv")

(defdata :data/root-role
  :euuid #uuid "601ee98d-796b-43f3-ac1f-881851407f34" :xid "CsRfQHNu3RyCgbpQQdanbd")

(defdata :data/public-role
  :euuid #uuid "746a7348-4daf-4b5a-921a-efc8bd476d88" :xid "FNnFqhZghyvTxtsfXP1wwM")

(defdata :data/public-user
  :euuid #uuid "762d9076-4b78-4918-9eec-262a56a94e95" :xid "FbQG7q3EYXeR7N3oBhd2Gk")

;; =============================================================================
;; System Data Records
;; =============================================================================
;; Records include BOTH :euuid and :xid keys for compatibility.
;; Use id/extract to get the appropriate ID based on current provider.

(def ^:dynamic *SYNTHIGY*
  {:euuid (id/data :data/synthigy-user :euuid)
   :xid (id/data :data/synthigy-user :xid)
   :name "Synthigy"
   :type :SERVICE
   :active true
   :modified_by {:euuid #uuid "c5a67922-351e-4ca3-95c2-fa52a7a3e2b5" :xid "RQb935cnLog5tiFsXgfMhv"}})

(def ^:dynamic *ROOT*
  {:euuid (id/data :data/root-role :euuid)
   :xid (id/data :data/root-role :xid)
   :name "SUPERUSER"
   :active true})

(def ^:dynamic *PUBLIC_ROLE*
  {:euuid (id/data :data/public-role :euuid)
   :xid (id/data :data/public-role :xid)
   :name "Public"})

(def ^:dynamic *PUBLIC_USER*
  {:euuid (id/data :data/public-user :euuid)
   :xid (id/data :data/public-user :xid)
   :name "__public__"
   :active false})
