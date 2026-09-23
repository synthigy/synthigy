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
