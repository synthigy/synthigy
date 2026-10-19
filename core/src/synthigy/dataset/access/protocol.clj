;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

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
