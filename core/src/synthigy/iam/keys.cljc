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

(ns synthigy.iam.keys
  "Well-known IAM entity/relation/data identity registrations, shared by backend
   and frontend."
  (:require
   #?(:clj  [synthigy.dataset.id :refer        [defentity defdata defrelation]]
      :cljs [synthigy.dataset.id :refer-macros [defentity defdata defrelation]])))

;; ============================================================================
;; IAM Entities
;; ============================================================================

(defentity :iam/user
  :euuid #uuid "edcab1db-ee6f-4744-bfea-447828893223" :xid "WN5xU8Do5pcdhTkxXvEYwt")

(defentity :iam/user-group
  :euuid #uuid "95afb558-3d28-45e5-9fbf-a2625afc5675" :xid "KV4seQm27gTAUHCxWERS12")

(defentity :iam/user-role
  :euuid #uuid "4778f7b1-f946-4cb2-b356-b9cb336b4087" :xid "9ptmMENqHWqLFbyidsyF9Y")

(defentity :iam/permission
  :euuid #uuid "6f525f5f-0504-498b-8b92-c353a0f9d141" :xid "EkJBUAxs1zQ5m2C1zXw4e4")

(defentity :iam/service-location
  :euuid #uuid "1029c9bd-dc48-436a-b7a8-2245508a4a72" :xid "2zmE3Enxdtzzs48a4DK6jB")

(defentity :iam/app
  :euuid #uuid "0757bd93-7abf-45b4-8437-2841283edcba" :xid "1ubBUFuDpcpqxY94TECvMw")

(defentity :iam/api
  :euuid #uuid "17b18b52-458e-4657-a827-eb1255de8f1e" :xid "3vhKU5p1QK9KPoyLoYkYW9")

(defentity :iam/scope
  :euuid #uuid "eb03406a-9b0c-4d61-8f96-34e8aa04f13c" :xid "W2BXVawY82expJoFJAAANX")

(defentity :iam/person-info
  :euuid #uuid "b2ccded3-9f8e-49aa-b610-8902ad7330e9" :xid "P5apo8kss7HEgtjdxXozwz")

(defentity :iam/user-public-profile
  :euuid #uuid "3aba5636-c2c5-4590-8bf4-216c3931ee97" :xid "8Fcd8LFoWmqPXk4BRGN9dQ")

(defentity :iam/auth-connector
  :euuid #uuid "e153782c-1993-44ce-a80d-4d2276c3361e" :xid "gn4FV5gXEQwiJQeRYfQjzZ")

;; ============================================================================
;; IAM Relations
;; ============================================================================

(defrelation :iam/user->roles
  :euuid #uuid "466b811e-0ec5-4871-a24d-5b2990e6db3d" :xid "9hMX1U9GJWbrZLgarrfnaG")

(defrelation :iam/user->groups
  :euuid #uuid "ae3e0f7f-dd0a-468c-9885-caac4141a5c3" :xid "NWwVJG6ympZP99zoS2E7F8")

(defrelation :iam/group->roles
  :euuid #uuid "ef549d07-5ba5-4c75-857e-bc1c673e3815" :xid "WZ79r1tzJEVKSzqmATEZFa")

;; ----------------------------------------------------------------------------
;; RBAC grant relations on User Role — CRUDOB on entities, :read/:write/:delete
;; on relations. `create entities` is the pre-CRUDOB `write entities` renamed,
;; so on an un-upgraded model this same identity still carries the merged grant.
;; ----------------------------------------------------------------------------

(defrelation :iam/role->create-entities
  :euuid #uuid "21bddba6-07c6-41e0-9c3c-788df053b5ac" :xid "5AfJz9QWtBLj918JvoLc9y")

(defrelation :iam/role->read-entities
  :euuid #uuid "3d1f4371-fcd1-4de9-b9a6-aa7ffafbeb83" :xid "8YmCJ4Yoo6W33RkKgrsr2e")

(defrelation :iam/role->update-entities
  :euuid #uuid "c8ac375b-ee0c-454a-ab27-dc36fbaed86d" :xid "RnEfKsjAExTJ3pmnctG2Rr")

(defrelation :iam/role->delete-entities
  :euuid #uuid "7205f259-c1c7-4ead-ad8d-25cab3bcf342" :xid "F5eNVDYvWEr4soqAU8JYS5")

(defrelation :iam/role->owned-entities
  :euuid #uuid "ad7b22c8-5370-4578-a749-7a73b7572c4a" :xid "NRVC6fP34PKC9mEKXaNt17")

(defrelation :iam/role->browse-entities
  :euuid #uuid "8a86314a-7cd8-42fd-ab49-885c928f8959" :xid "J786Xy2p7j4qf1Qt3gYC5W")

(defrelation :iam/role->from-read-relations
  :euuid #uuid "25c97af4-4ddb-48e1-9fe9-4778999de49c" :xid "5fdmrbTYYQUXahV7QT95wH")

(defrelation :iam/role->from-write-relations
  :euuid #uuid "1deeb442-5bd2-45e3-8ba0-5cbb1d8d5389" :xid "4hNy4RPb35p4bvatdLUPZA")

(defrelation :iam/role->from-delete-relations
  :euuid #uuid "9c8ebcd4-5b23-4619-8fe8-cd3f474e7f72" :xid "LLHBTRnWWb7DtGCmP8z4o3")

(defrelation :iam/role->to-read-relations
  :euuid #uuid "4f4cd8f9-e76a-45c0-8bcd-33b13a20e5fe" :xid "AnxP7t9R6WmFic7piVxmYH")

(defrelation :iam/role->to-write-relations
  :euuid #uuid "1c7b9658-aaae-409a-821a-f0bf31dd44d6" :xid "4Wzmf2NFacsaQo9vZi6MeD")

(defrelation :iam/role->to-delete-relations
  :euuid #uuid "9fd134da-ac87-4f48-aa7a-d94ae6ba18be" :xid "LjdEukYQUMdPduAcspH1kH")

;; ============================================================================
;; IAM Data
;; ============================================================================

(defdata :iam/id
  :euuid #uuid "c5c85417-0aef-4c44-9e86-8090647d6378" :xid "RRY5JcXr8MwppjvK9bhp7Z")

;; The column /oauth/login authenticates against — see iam/access.md
(defdata :iam.user/password
  :euuid #uuid "2c5684ac-d8e1-40a9-8a4b-db602052907f" :xid "6UZ37fazSUWp4XwqA2CiFg")

(defdata :iam.model/version-0.80.0
  :euuid #uuid "7c9981ed-8494-47b0-9580-adc2951819f9" :xid "GPPpgwJqAuSEbRRuoMYLLg")

(defrelation :oauth/user->person-info
  :xid "BfRjYxqSLbdbGXJNRAnyfH")

(defrelation :oauth/user->public-profile
  :xid "Bopx5916jrSf7HVxipaiat")
