(ns synthigy.dataset.access.protocol
  "Access control protocol definition.

  This namespace contains only the protocol definition to avoid naming
  collisions with convenience functions in synthigy.dataset.access.")

;;; ============================================================================
;;; Access Control Protocol
;;; ============================================================================

(defprotocol AccessControl
  "Protocol for dataset-level access control.

  Implementations should determine whether the current context (user, roles, groups)
  is allowed to perform operations on dataset entities and relations."

  (entity-allows?
    [this entity-id operations]
    "Check if access is allowed for entity operations.

    Args:
      this       - Access control implementation
      entity-id  - UUID of the entity
      operations - Set of operations (#{:read :write :delete})

    Returns: boolean - true if allowed, false otherwise")

  (relation-allows?
    [this relation-id operations]
    [this relation-id from-to operations]
    "Check if access is allowed for relation operations.

    Args:
      this       - Access control implementation
      relation-id - UUID of the relation
      from-to    - Optional vector of [from-uuid to-uuid] for directional checks
      operations - Set of operations (#{:read :write :delete})

    Returns: boolean - true if allowed, false otherwise")

  (scope-allowed?
    [this scope]
    "Check if a specific scope is allowed for the current context.

    Args:
      this  - Access control implementation
      scope - String scope identifier (e.g., \"dataset:deploy\")

    Returns: boolean - true if allowed, false otherwise")

  (roles-allowed?
    [this role-ids]
    "Check if the current context has any of the specified roles.

    Args:
      this     - Access control implementation
      role-ids - Collection of role UUIDs

    Returns: boolean - true if any role matches, false otherwise")

  (superuser?
    [this]
    "Check if the current context represents a superuser.

    Superusers bypass all access control checks. This typically includes
    service accounts and users with administrative roles.

    Args:
      this - Access control implementation

    Returns: boolean - true if superuser, false otherwise")

  (get-user
    [this]
    "Get the current user from the access control context.

    Returns: User entity map with :_eid, :euuid, :name, etc., or nil if no user")

  (get-roles
    [this]
    "Get the current roles from the access control context.

    Returns: Set of role UUIDs, or empty set if no roles")

  (get-groups
    [this]
    "Get the current groups from the access control context.

    Returns: Set of group UUIDs, or empty set if no groups"))
