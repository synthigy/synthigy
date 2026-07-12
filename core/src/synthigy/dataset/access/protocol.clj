(ns synthigy.dataset.access.protocol
  "Access control protocol definition.

  This namespace contains only the protocol definition to avoid naming
  collisions with convenience functions in synthigy.dataset.access.

  Identifier discipline:
    role-ids  → set of role id-keys (xids) for RBAC table lookups.
    group-eids → set of group :_eid values for RLS predicates.

  RBAC keys on stable id-keys (xids cross deploys and appear in token
  claims). RLS joins against bigint membership columns, so groups
  project to :_eid sets to match. There is intentionally no role-eids
  or group-ids accessor — the two concerns use distinct identifier
  forms by design.")

(defprotocol AccessControl
  "Protocol for dataset-level access control. Implementations decide whether the current principal can act on entities/relations."

  (entity-allows? [this entity-id operations])
  (relation-allows?
    [this relation-id operations]
    [this relation-id from-to operations])
  (scope-allowed? [this scope])
  (roles-allowed? [this role-ids])
  (superuser? [this])

  (get-principal [this]
    "Materialized principal map (see synthigy.iam.context/get-user-details for shape), or nil.")

  (principal-eid [this]
    "Principal's :_eid, or nil.")

  (role-ids [this]
    "Set of role id-keys (xids) for the current principal, or #{}. RBAC.")

  (group-eids [this]
    "Set of group :_eid values for the current principal, or #{}. RLS."))
