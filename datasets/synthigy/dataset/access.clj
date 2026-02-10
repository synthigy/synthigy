(ns synthigy.dataset.access
  "Access control convenience functions for dataset operations.

  This namespace provides convenience functions that delegate to the current
  access control implementation. The actual protocol is defined in
  synthigy.dataset.access.protocol.

  This design allows:
  - Datasets to work without IAM (using default allow-all implementation)
  - IAM to plug in access control when available
  - Testing with mock access control implementations
  - No circular dependency between datasets and IAM"
  (:require
   [synthigy.dataset.access.protocol :as p]))

;;; ============================================================================
;;; Default Implementation (Allow All)
;;; ============================================================================

(defrecord AllowAllAccess []
  p/AccessControl
  (entity-allows? [_ _ _]
    true)

  (relation-allows? [_ _ _]
    true)

  (relation-allows? [_ _ _ _]
    true)

  (scope-allowed? [_ _]
    true)

  (roles-allowed? [_ _]
    true)

  (superuser? [_]
    true)

  (get-user [_]
    nil)

  (get-roles [_]
    #{})

  (get-groups [_]
    #{}))

;;; ============================================================================
;;; Dynamic Context
;;; ============================================================================

;; Dynamic var holding the current access control implementation.
;; Defaults to AllowAllAccess which permits all operations.
;; IAM layer should bind this to an IAM-aware implementation during request processing.
(defonce ^:dynamic *access-control* (->AllowAllAccess))

;;; ============================================================================
;;; Convenience Functions
;;; ============================================================================

(defn entity-allows?
  "Check if current access context allows entity operations.

  Uses the dynamic *access-control* var."
  [entity-id operations]
  (p/entity-allows? *access-control* entity-id operations))

(defn relation-allows?
  "Check if current access context allows relation operations.

  Uses the dynamic *access-control* var."
  ([relation-id operations]
   (p/relation-allows? *access-control* relation-id operations))
  ([relation-id from-to operations]
   (p/relation-allows? *access-control* relation-id from-to operations)))

(defn scope-allowed?
  "Check if current access context allows a scope.

  Uses the dynamic *access-control* var."
  [scope]
  (p/scope-allowed? *access-control* scope))

(defn roles-allowed?
  "Check if current access context has any of the specified roles.

  Uses the dynamic *access-control* var."
  [role-ids]
  (p/roles-allowed? *access-control* role-ids))

(defn superuser?
  "Check if current access context is a superuser.

  Uses the dynamic *access-control* var."
  []
  (p/superuser? *access-control*))

(defn current-user
  "Get current user from access context.

  Uses the dynamic *access-control* var."
  []
  (p/get-user *access-control*))

(defn current-roles
  "Get current roles from access context.

  Uses the dynamic *access-control* var."
  []
  (p/get-roles *access-control*))

(defn current-groups
  "Get current groups from access context.

  Uses the dynamic *access-control* var."
  []
  (p/get-groups *access-control*))

;;; ============================================================================
;;; Legacy Compatibility (Dynamic Vars)
;;; ============================================================================

;; These provide backward compatibility with code that uses dynamic vars directly.
;; DEPRECATED: Use current-user, current-roles, current-groups functions instead.

(defonce ^:dynamic *user* nil)
(defonce ^:dynamic *roles* #{})
(defonce ^:dynamic *groups* #{})
