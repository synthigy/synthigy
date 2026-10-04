;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.access.protocol
  "Access control protocol definitions.")

(defprotocol AccessControl
  "Dataset-level access control decisions for the current principal."

  (entity-allows? [this entity-id operations])
  (relation-allows?
    [this relation-id operations]
    [this relation-id from-to operations])
  (attribute-allows? [this entity-id attribute-id op]
    "Deny-list check: may the principal perform op (:read / :write) on attribute-id of entity-id?")
  (scope-allowed? [this scope])
  (roles-allowed? [this role-ids])
  (superuser? [this])

  (get-principal [this]
    "Materialized principal map (see synthigy.iam.context/get-user-details for shape), or nil.")

  (principal-eid [this]
    "Principal's :_eid, or nil.")

  (role-ids [this]
    "Set of role id-keys (xids) for the current principal, or #{}. RBAC.")

  (role-eids [this]
    "Set of role :_eid values for the current principal, or #{}. RLS.")

  (group-eids [this]
    "Set of group :_eid values for the current principal, or #{}. RLS."))

(defprotocol RLSBypass
  "Role-held row-scope bypass; kept separate from AccessControl so
   non-implementing stubs stay fail-closed."
  (rls-bypass? [this entity-id operation]
    "True when the principal's roles grant a row-scope bypass on entity-id for operation."))
