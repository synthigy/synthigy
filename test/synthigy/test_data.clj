(ns synthigy.test-data
  "Well-known test data IDs for provider-agnostic tests.

   This namespace registers all test data IDs using defdata, enabling
   tests to work with both UUID and NanoID providers after XID migration.

   Usage:
     (require '[synthigy.test-data])  ; Load all registrations
     (require '[synthigy.dataset.id :as id])

     ;; Get ID using current provider
     (id/data :test/human-grandma)  ;; => UUID or NanoID based on provider

   XID Convention:
     - 22 characters (Base58-encoded UUID, deterministic via uuid->nanoid)
     - Each XID is the deterministic encoding of its EUUID"
  (:require [synthigy.dataset.id :refer [defdata]]))

;;; ============================================================================
;;; Humanity Test Suite (humanity_test.clj)
;;; ============================================================================
;;; Entity: Human (self-referential with tree/o2m relations)

;; Entity schema ID
(defdata :test/human-entity
  :euuid #uuid "c39a54ab-f729-4c2d-af88-22cc130c9512" :xid "R9vdjiSfQxoEwMN83gVbNy")

;; Test humans - family members
(defdata :test/human-grandma
  :euuid #uuid "00000001-0000-0000-0000-000000000001" :xid "111115qCHTcgbQwpvYZQ9e")

(defdata :test/human-grandpa
  :euuid #uuid "00000001-0000-0000-0000-000000000002" :xid "111115qCHTcgbQwpvYZQ9f")

(defdata :test/human-mother
  :euuid #uuid "00000001-0000-0000-0000-000000000003" :xid "111115qCHTcgbQwpvYZQ9g")

(defdata :test/human-father
  :euuid #uuid "00000001-0000-0000-0000-000000000004" :xid "111115qCHTcgbQwpvYZQ9h")

(defdata :test/human-aunt
  :euuid #uuid "00000001-0000-0000-0000-000000000005" :xid "111115qCHTcgbQwpvYZQ9i")

(defdata :test/human-child1
  :euuid #uuid "00000001-0000-0000-0000-000000000006" :xid "111115qCHTcgbQwpvYZQ9j")

(defdata :test/human-child2
  :euuid #uuid "00000001-0000-0000-0000-000000000007" :xid "111115qCHTcgbQwpvYZQ9k")

(defdata :test/human-child3
  :euuid #uuid "00000001-0000-0000-0000-000000000008" :xid "111115qCHTcgbQwpvYZQ9m")

(defdata :test/human-child4
  :euuid #uuid "00000001-0000-0000-0000-000000000009" :xid "111115qCHTcgbQwpvYZQ9n")

(defdata :test/human-ext-friend
  :euuid #uuid "00000001-0000-0000-0000-00000000000a" :xid "111115qCHTcgbQwpvYZQ9o")

;; Dynamic test humans (temporary entities)
(defdata :test/human-temp1
  :euuid #uuid "00000001-0000-0000-0000-000000000101" :xid "111115qCHTcgbQwpvYZQE4")

(defdata :test/human-temp2
  :euuid #uuid "00000001-0000-0000-0000-000000000102" :xid "111115qCHTcgbQwpvYZQE5")

(defdata :test/human-temp3
  :euuid #uuid "00000001-0000-0000-0000-000000000103" :xid "111115qCHTcgbQwpvYZQE6")

(defdata :test/human-temp4
  :euuid #uuid "00000001-0000-0000-0000-000000000104" :xid "111115qCHTcgbQwpvYZQE7")

(defdata :test/human-temp5
  :euuid #uuid "00000001-0000-0000-0000-000000000105" :xid "111115qCHTcgbQwpvYZQE8")

;;; ============================================================================
;;; Organization Test Suite (organization_test.clj)
;;; ============================================================================
;;; Entity: Employee (self-referential manager hierarchy)

;; Entity schema ID
(defdata :test/employee-entity
  :euuid #uuid "74be523f-1992-4629-9ba2-9b07a4d8d836" :xid "FR8MJAboRh8nQQBqzTEV9o")

;; Test employees - org hierarchy
(defdata :test/employee-ceo
  :euuid #uuid "00000002-0000-0000-0000-000000000001" :xid "11111AfPZvENBpter67oJG")

(defdata :test/employee-vp-eng
  :euuid #uuid "00000002-0000-0000-0000-000000000002" :xid "11111AfPZvENBpter67oJH")

(defdata :test/employee-vp-sales
  :euuid #uuid "00000002-0000-0000-0000-000000000003" :xid "11111AfPZvENBpter67oJJ")

(defdata :test/employee-vp-hr
  :euuid #uuid "00000002-0000-0000-0000-000000000004" :xid "11111AfPZvENBpter67oJK")

(defdata :test/employee-dir-platform
  :euuid #uuid "00000002-0000-0000-0000-000000000005" :xid "11111AfPZvENBpter67oJL")

(defdata :test/employee-dir-product
  :euuid #uuid "00000002-0000-0000-0000-000000000006" :xid "11111AfPZvENBpter67oJM")

(defdata :test/employee-senior-eng
  :euuid #uuid "00000002-0000-0000-0000-000000000007" :xid "11111AfPZvENBpter67oJN")

(defdata :test/employee-engineer
  :euuid #uuid "00000002-0000-0000-0000-000000000008" :xid "11111AfPZvENBpter67oJP")

(defdata :test/employee-sales-rep
  :euuid #uuid "00000002-0000-0000-0000-000000000009" :xid "11111AfPZvENBpter67oJQ")

(defdata :test/employee-recruiter
  :euuid #uuid "00000002-0000-0000-0000-00000000000a" :xid "11111AfPZvENBpter67oJR")

;; Dynamic test employees (temporary entities)
(defdata :test/employee-temp1
  :euuid #uuid "00000002-0000-0000-0000-000000000101" :xid "11111AfPZvENBpter67oNg")

(defdata :test/employee-temp2
  :euuid #uuid "00000002-0000-0000-0000-000000000102" :xid "11111AfPZvENBpter67oNh")

(defdata :test/employee-temp3
  :euuid #uuid "00000002-0000-0000-0000-000000000103" :xid "11111AfPZvENBpter67oNi")

(defdata :test/employee-temp4
  :euuid #uuid "00000002-0000-0000-0000-000000000104" :xid "11111AfPZvENBpter67oNj")

(defdata :test/employee-temp5
  :euuid #uuid "00000002-0000-0000-0000-000000000105" :xid "11111AfPZvENBpter67oNk")

;;; ============================================================================
;;; Read Test Suite (read_test.clj)
;;; ============================================================================
;;; Uses IAM entities: User, UserRole

;; Test users
(defdata :test/user-alice
  :euuid #uuid "00000000-0000-0000-0001-000000000001" :xid "11111111111113CUsUpv9v")

(defdata :test/user-bob
  :euuid #uuid "00000000-0000-0000-0001-000000000002" :xid "11111111111113CUsUpv9w")

(defdata :test/user-charlie
  :euuid #uuid "00000000-0000-0000-0001-000000000003" :xid "11111111111113CUsUpv9x")

(defdata :test/user-null-name
  :euuid #uuid "00000000-0000-0000-0001-000000000004" :xid "11111111111113CUsUpv9y")

;; Test roles
(defdata :test/role-admin
  :euuid #uuid "00000000-0000-0000-0002-000000000001" :xid "11111111111115PxjxeqJp")

(defdata :test/role-viewer
  :euuid #uuid "00000000-0000-0000-0002-000000000002" :xid "11111111111115PxjxeqJq")

;;; ============================================================================
;;; Audit Test Suite (created_fields_test.clj)
;;; ============================================================================

(defdata :test/audit-creator
  :euuid #uuid "00000000-0000-0000-0000-000000000101" :xid "111111111111111111115S")

(defdata :test/audit-updater
  :euuid #uuid "00000000-0000-0000-0000-000000000102" :xid "111111111111111111115T")

(defdata :test/audit-test-user
  :euuid #uuid "00000000-0000-0000-0000-000000000103" :xid "111111111111111111115U")

;;; ============================================================================
;;; Audit Test Suite - Postgres Tests (postgres_test.clj)
;;; ============================================================================

(defdata :test/audit-modifier
  :euuid #uuid "00000000-0000-0000-0000-000000000001" :xid "111111111111111111114i")

(defdata :test/audit-creator-pg
  :euuid #uuid "00000000-0000-0000-0000-000000000002" :xid "111111111111111111114j")

(defdata :test/audit-user-1
  :euuid #uuid "00000000-0000-0000-0000-000000000011" :xid "111111111111111111115C")

(defdata :test/audit-user-2
  :euuid #uuid "00000000-0000-0000-0000-000000000012" :xid "111111111111111111115D")

(defdata :test/audit-updater-pg
  :euuid #uuid "00000000-0000-0000-0000-000000000003" :xid "111111111111111111114k")

;;; ============================================================================
;;; Common Test Data
;;; ============================================================================

(defdata :test/nonexistent-entity
  :euuid #uuid "99999999-9999-9999-9999-999999999999" :xid "Ky6bybkEghtb9bgYzVbsKK")

;;; ============================================================================
;;; Deployment Test Suite (deployment_test.clj)
;;; ============================================================================
;;; Test datasets with their entities, attributes, and relations

;; Dataset A
(defdata :test/dataset-a
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000" :xid "N5L7e7PGCtssqDkwszYyoD")

(defdata :test/dataset-a-v1
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000001" :xid "N5L7e7PGCtssqDkwszYyoE")

;; Dataset A Entities
(defdata :test/product-entity
  :euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001" :xid "N5L7e7hwcEWL8doUwPGYZn")

(defdata :test/category-entity
  :euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002" :xid "N5L7e7hwcEWL8doUwPGYZo")

;; Dataset A Attributes
(defdata :test/product-name-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001" :xid "N5L7e7hwdC7okUoqAXAtue")

(defdata :test/product-price-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000002" :xid "N5L7e7hwdC7okUoqAXAtuf")

(defdata :test/category-name-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003" :xid "N5L7e7hwdC7okUoqAXAtug")

;; Dataset A Relations
(defdata :test/product-category-rel
  :euuid #uuid "aaaaaaaa-2222-0000-0000-000000000001" :xid "N5L7e82d1a8nS3r1zmz7LL")

;; Dataset B
(defdata :test/dataset-b
  :euuid #uuid "bbbbbbbb-0000-0000-0000-000000000000" :xid "QBZdJd2Hj5sAWqqLmVWNsq")

(defdata :test/dataset-b-v1
  :euuid #uuid "bbbbbbbb-0000-0000-0000-000000000001" :xid "QBZdJd2Hj5sAWqqLmVWNsr")

;; Dataset B Entities (Product is shared with A)
(defdata :test/order-entity
  :euuid #uuid "bbbbbbbb-1111-0000-0000-000000000003" :xid "QBZdJdLy8RVcpFssptDweS")

;; Dataset B Attributes
(defdata :test/product-description-attr
  :euuid #uuid "bbbbbbbb-1111-1111-0000-000000000004" :xid "QBZdJdLy9P76S6tE428HzK")

(defdata :test/order-number-attr
  :euuid #uuid "bbbbbbbb-1111-1111-0000-000000000005" :xid "QBZdJdLy9P76S6tE428HzL")

;; Dataset B Relations
(defdata :test/order-product-rel
  :euuid #uuid "bbbbbbbb-2222-0000-0000-000000000001" :xid "QBZdJdfeXm857fvQtGwWQx")

;; Dataset C
(defdata :test/dataset-c
  :euuid #uuid "cccccccc-0000-0000-0000-000000000000" :xid "SHo8y8fKFGrTCTujezTmxT")

(defdata :test/dataset-c-v1
  :euuid #uuid "cccccccc-0000-0000-0000-000000000001" :xid "SHo8y8fKFGrTCTujezTmxU")

;; Dataset C Attributes
(defdata :test/product-sku-attr
  :euuid #uuid "cccccccc-1111-1111-0000-000000000001" :xid "SHo8y8yzfa6P7ixcwX5h4t")

;; Dataset D
(defdata :test/dataset-d
  :euuid #uuid "dddddddd-0000-0000-0000-000000000000" :xid "UQ2edeJLmTqjt5z8YVRB35")

(defdata :test/dataset-d-v1
  :euuid #uuid "dddddddd-0000-0000-0000-000000000001" :xid "UQ2edeJLmTqjt5z8YVRB36")

;; Dataset D Entities
(defdata :test/supplier-entity
  :euuid #uuid "dddddddd-1111-0000-0000-000000000001" :xid "UQ2eded2AoUCBW2fbt8joe")

;; Dataset D Attributes
(defdata :test/supplier-name-attr
  :euuid #uuid "dddddddd-1111-1111-0000-000000000001" :xid "UQ2eded2Bm5foM31q2369W")

;; Dataset D Relations
(defdata :test/product-supplier-rel
  :euuid #uuid "dddddddd-2222-0000-0000-000000000001" :xid "UQ2edewha96eUv5CfGrJaC")

;; Dataset A v2 - additional attribute for renamed entity
(defdata :test/dataset-a-v2
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000002" :xid "N5L7e7PGCtssqDkwszYyoF")

(defdata :test/item-description-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000010" :xid "N5L7e7hwdC7okUoqAXAtuu")

;; Dataset A v3-v12 (for schema evolution tests)
(defdata :test/dataset-a-v3
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000003" :xid "N5L7e7PGCtssqDkwszYyoG")

(defdata :test/dataset-a-v4
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000004" :xid "N5L7e7PGCtssqDkwszYyoH")

(defdata :test/dataset-a-v5
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000005" :xid "N5L7e7PGCtssqDkwszYyoJ")

(defdata :test/dataset-a-v6
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000006" :xid "N5L7e7PGCtssqDkwszYyoK")

(defdata :test/dataset-a-v7
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000007" :xid "N5L7e7PGCtssqDkwszYyoL")

(defdata :test/dataset-a-v8
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000008" :xid "N5L7e7PGCtssqDkwszYyoM")

(defdata :test/dataset-a-v9
  :euuid #uuid "aaaaaaaa-0000-0000-0000-000000000009" :xid "N5L7e7PGCtssqDkwszYyoN")

(defdata :test/dataset-a-v10
  :euuid #uuid "aaaaaaaa-0000-0000-0000-00000000000a" :xid "N5L7e7PGCtssqDkwszYyoP")

(defdata :test/dataset-a-v11
  :euuid #uuid "aaaaaaaa-0000-0000-0000-00000000000b" :xid "N5L7e7PGCtssqDkwszYyoQ")

(defdata :test/dataset-a-v12
  :euuid #uuid "aaaaaaaa-0000-0000-0000-00000000000c" :xid "N5L7e7PGCtssqDkwszYyoR")

;; Additional entities and attributes for schema evolution
(defdata :test/comment-entity
  :euuid #uuid "aaaaaaaa-1111-0000-0000-000000000003" :xid "N5L7e7hwcEWL8doUwPGYZp")

(defdata :test/comment-text-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000020" :xid "N5L7e7hwdC7okUoqAXAtv4")

(defdata :test/comment-rating-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000021" :xid "N5L7e7hwdC7okUoqAXAtv5")

(defdata :test/item-status-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000030" :xid "N5L7e7hwdC7okUoqAXAvt6")

(defdata :test/comment-priority-attr
  :euuid #uuid "aaaaaaaa-1111-1111-0000-000000000040" :xid "N5L7e7hwdC7okUoqAXAvtA")

(defdata :test/comment-item-rel
  :euuid #uuid "aaaaaaaa-2222-0000-0000-000000000002" :xid "N5L7e82d1a8nS3r1zmz7LM")

;; Enum values for status attribute
(defdata :test/status-draft
  :euuid #uuid "aaaaaaaa-3333-3333-0000-000000000001" :xid "N5L7e8BCTYDmJSQUn8ojfe")

(defdata :test/status-published
  :euuid #uuid "aaaaaaaa-3333-3333-0000-000000000002" :xid "N5L7e8BCTYDmJSQUn8ojff")

(defdata :test/status-archived
  :euuid #uuid "aaaaaaaa-3333-3333-0000-000000000003" :xid "N5L7e8BCTYDmJSQUn8ojfg")

;; Enum values for priority attribute
(defdata :test/priority-low
  :euuid #uuid "aaaaaaaa-4444-4444-0000-000000000001" :xid "N5L7e8D4MfYFrh3cN3ZYt7")

(defdata :test/priority-medium
  :euuid #uuid "aaaaaaaa-4444-4444-0000-000000000002" :xid "N5L7e8D4MfYFrh3cN3ZYt8")

(defdata :test/priority-high
  :euuid #uuid "aaaaaaaa-4444-4444-0000-000000000003" :xid "N5L7e8D4MfYFrh3cN3ZYt9")

;; Schema evolution test datasets (v9.x)
(defdata :test/evolution-dataset
  :euuid #uuid "99999999-0000-0000-0000-000000000000" :xid "Ky6bybkEghtb9bgYzVbaib")

(defdata :test/evolution-v1
  :euuid #uuid "99999999-0000-0000-0000-000000000001" :xid "Ky6bybkEghtb9bgYzVbaic")

(defdata :test/evolution-v2
  :euuid #uuid "99999999-0000-0000-0000-000000000002" :xid "Ky6bybkEghtb9bgYzVbaid")

(defdata :test/evolution-v3
  :euuid #uuid "99999999-0000-0000-0000-000000000003" :xid "Ky6bybkEghtb9bgYzVbaie")

;; Evolution entity
(defdata :test/evolution-entity
  :euuid #uuid "99999999-1111-0000-0000-000000000001" :xid "Ky6byc4v63X3T1j63tK9VA")

;; Evolution attributes
(defdata :test/evolution-name-attr
  :euuid #uuid "99999999-1111-1111-0000-000000000001" :xid "Ky6byc4v718X4rjSH2DVq2")

(defdata :test/evolution-desc-attr
  :euuid #uuid "99999999-1111-1111-0000-000000000002" :xid "Ky6byc4v718X4rjSH2DVq3")

(defdata :test/evolution-status-attr
  :euuid #uuid "99999999-1111-1111-0000-000000000003" :xid "Ky6byc4v718X4rjSH2DVq4")

;; Evolution relations
(defdata :test/evolution-parent-rel
  :euuid #uuid "99999999-2222-0000-0000-000000000001" :xid "Ky6bycPbVP9VkRmd7H2iFi")

;;; ============================================================================
;;; OAuth Test Suite
;;; ============================================================================
;;; Test clients, users, and sessions for OAuth flow tests

;; Device Code Flow Test (device_code_flow_test.clj)
(defdata :test/oauth-device-client
  :euuid #uuid "10000001-0000-0000-0000-000000000001" :xid "2ybRBpRJ3Hq7xatVsoBEcG")

(defdata :test/oauth-device-wrong-client
  :euuid #uuid "10000001-0000-0000-0000-000000000002" :xid "2ybRBpRJ3Hq7xatVsoBEcH")

(defdata :test/oauth-device-user
  :euuid #uuid "10000001-0000-0000-0000-000000000003" :xid "2ybRBpRJ3Hq7xatVsoBEcJ")

(defdata :test/oauth-device-session
  :euuid #uuid "10000001-0000-0000-0000-000000000004" :xid "2ybRBpRJ3Hq7xatVsoBEcK")

;; PKCE Security Test (security_pkce_test.clj)
(defdata :test/oauth-pkce-client
  :euuid #uuid "10000002-0000-0000-0000-000000000001" :xid "2ybRBuFVKkSoYzqKoLjdkt")

(defdata :test/oauth-pkce-user
  :euuid #uuid "10000002-0000-0000-0000-000000000002" :xid "2ybRBuFVKkSoYzqKoLjdku")

;; Refresh Token Flow Test (refresh_token_flow_test.clj)
(defdata :test/oauth-refresh-client
  :euuid #uuid "10000003-0000-0000-0000-000000000001" :xid "2ybRBz5gcD4V9Qn9itJ2uW")

(defdata :test/oauth-refresh-user
  :euuid #uuid "10000003-0000-0000-0000-000000000002" :xid "2ybRBz5gcD4V9Qn9itJ2uX")

;; Code Expiration Test (security_code_expiration_test.clj)
(defdata :test/oauth-expire-client
  :euuid #uuid "10000004-0000-0000-0000-000000000001" :xid "2ybRC4ustfgAjpiyeRrS48")

(defdata :test/oauth-expire-user
  :euuid #uuid "10000004-0000-0000-0000-000000000002" :xid "2ybRC4ustfgAjpiyeRrS49")

;; Redirect URI Security Test (security_redirect_uri_test.clj)
(defdata :test/oauth-redirect-client
  :euuid #uuid "10000005-0000-0000-0000-000000000001" :xid "2ybRC9k5B8HrLEfoZyQqCk")

(defdata :test/oauth-redirect-user
  :euuid #uuid "10000005-0000-0000-0000-000000000002" :xid "2ybRC9k5B8HrLEfoZyQqCm")

;; Client Credentials Flow Test (client_credentials_flow_test.clj)
(defdata :test/oauth-creds-client
  :euuid #uuid "10000006-0000-0000-0000-000000000001" :xid "2ybRCEaGTauXvecdVWyEMN")

(defdata :test/oauth-creds-restricted
  :euuid #uuid "10000006-0000-0000-0000-000000000002" :xid "2ybRCEaGTauXvecdVWyEMP")

;; Client Grant Test - Audience & Scope Filtering (client_grant_test.clj)
(defdata :test/client-grant-api
  :euuid #uuid "10000006-0000-0000-0000-000000000010" :xid "2ybRCEaGTauXvecdVWyEN1")

(defdata :test/client-grant-role
  :euuid #uuid "10000006-0000-0000-0000-000000000011" :xid "2ybRCEaGTauXvecdVWyEN2")

(defdata :test/client-grant-client
  :euuid #uuid "10000006-0000-0000-0000-000000000012" :xid "2ybRCEaGTauXvecdVWyEN3")

(defdata :test/client-grant-service-user
  :euuid #uuid "10000006-0000-0000-0000-000000000013" :xid "2ybRCEaGTauXvecdVWyEN4")

;; Authorization Code Flow Test (authorization_code_flow_test.clj)
(defdata :test/oauth-authcode-client
  :euuid #uuid "10000007-0000-0000-0000-000000000001" :xid "2ybRCKQTk3XDX4ZTR4XdVz")

(defdata :test/oauth-authcode-user
  :euuid #uuid "10000007-0000-0000-0000-000000000002" :xid "2ybRCKQTk3XDX4ZTR4XdW1")

;; OAuth Integration Test (integration_test.clj)
(defdata :test/oauth-integ-client-1
  :euuid #uuid "10000008-0000-0000-0000-000000000001" :xid "2ybRCQEf2W8u7UWHLc62ec")

(defdata :test/oauth-integ-client-2
  :euuid #uuid "10000008-0000-0000-0000-000000000002" :xid "2ybRCQEf2W8u7UWHLc62ed")

(defdata :test/oauth-integ-client-3
  :euuid #uuid "10000008-0000-0000-0000-000000000003" :xid "2ybRCQEf2W8u7UWHLc62ee")

(defdata :test/oauth-integ-user-1
  :euuid #uuid "10000008-0000-0000-0000-000000000011" :xid "2ybRCQEf2W8u7UWHLc62et")

(defdata :test/oauth-integ-user-2
  :euuid #uuid "10000008-0000-0000-0000-000000000012" :xid "2ybRCQEf2W8u7UWHLc62eu")

(defdata :test/oauth-integ-user-3
  :euuid #uuid "10000008-0000-0000-0000-000000000013" :xid "2ybRCQEf2W8u7UWHLc62ev")

;; General OAuth Test (oauth_test.clj)
(defdata :test/oauth-client-1
  :euuid #uuid "10000009-0000-0000-0000-000000000001" :xid "2ybRCV4rJxkahtT7G9eRoE")

(defdata :test/oauth-client-2
  :euuid #uuid "10000009-0000-0000-0000-000000000002" :xid "2ybRCV4rJxkahtT7G9eRoF")

(defdata :test/oauth-client-3
  :euuid #uuid "10000009-0000-0000-0000-000000000003" :xid "2ybRCV4rJxkahtT7G9eRoG")

(defdata :test/oauth-client-4
  :euuid #uuid "10000009-0000-0000-0000-000000000004" :xid "2ybRCV4rJxkahtT7G9eRoH")

(defdata :test/oauth-client-5
  :euuid #uuid "10000009-0000-0000-0000-000000000005" :xid "2ybRCV4rJxkahtT7G9eRoJ")

(defdata :test/oauth-client-6
  :euuid #uuid "10000009-0000-0000-0000-000000000006" :xid "2ybRCV4rJxkahtT7G9eRoK")

(defdata :test/oauth-client-7
  :euuid #uuid "10000009-0000-0000-0000-000000000007" :xid "2ybRCV4rJxkahtT7G9eRoL")

(defdata :test/oauth-user-1
  :euuid #uuid "10000009-0000-0000-0000-000000000101" :xid "2ybRCV4rJxkahtT7G9eRse")

(defdata :test/oauth-user-2
  :euuid #uuid "10000009-0000-0000-0000-000000000102" :xid "2ybRCV4rJxkahtT7G9eRsf")

(defdata :test/oauth-user-3
  :euuid #uuid "10000009-0000-0000-0000-000000000103" :xid "2ybRCV4rJxkahtT7G9eRsg")

(defdata :test/oauth-user-4
  :euuid #uuid "10000009-0000-0000-0000-000000000104" :xid "2ybRCV4rJxkahtT7G9eRsh")

(defdata :test/oauth-user-5
  :euuid #uuid "10000009-0000-0000-0000-000000000105" :xid "2ybRCV4rJxkahtT7G9eRsi")

;;; ============================================================================
;;; IAM Test Suite
;;; ============================================================================
;;; Test users, roles, groups, and scopes for IAM tests

;; Context Test (context_test.clj)
(defdata :test/iam-context-user-1
  :euuid #uuid "20000001-0000-0000-0000-000000000001" :xid "4xBqNZ1Po83ZKkqAq3o54t")

(defdata :test/iam-context-user-2
  :euuid #uuid "20000001-0000-0000-0000-000000000002" :xid "4xBqNZ1Po83ZKkqAq3o54u")

(defdata :test/iam-context-user-3
  :euuid #uuid "20000001-0000-0000-0000-000000000003" :xid "4xBqNZ1Po83ZKkqAq3o54v")

(defdata :test/iam-context-role-1
  :euuid #uuid "20000001-0000-0000-0000-000000000101" :xid "4xBqNZ1Po83ZKkqAq3o59J")

(defdata :test/iam-context-group-1
  :euuid #uuid "20000001-0000-0000-0000-000000000201" :xid "4xBqNZ1Po83ZKkqAq3o5Di")

(defdata :test/iam-context-user-4
  :euuid #uuid "20000001-0000-0000-0000-000000000004" :xid "4xBqNZ1Po83ZKkqAq3o54w")

(defdata :test/iam-context-user-5
  :euuid #uuid "20000001-0000-0000-0000-000000000005" :xid "4xBqNZ1Po83ZKkqAq3o54x")

(defdata :test/iam-context-role-2
  :euuid #uuid "20000001-0000-0000-0000-000000000102" :xid "4xBqNZ1Po83ZKkqAq3o59K")

(defdata :test/iam-context-group-2
  :euuid #uuid "20000001-0000-0000-0000-000000000202" :xid "4xBqNZ1Po83ZKkqAq3o5Dj")

;; Access Test (access_test.clj)
(defdata :test/iam-access-user-1
  :euuid #uuid "20000002-0000-0000-0000-000000000001" :xid "4xBqNdqb5afEvAmzkbMUDW")

(defdata :test/iam-access-role-1
  :euuid #uuid "20000002-0000-0000-0000-000000000101" :xid "4xBqNdqb5afEvAmzkbMUHv")

(defdata :test/iam-access-role-2
  :euuid #uuid "20000002-0000-0000-0000-000000000102" :xid "4xBqNdqb5afEvAmzkbMUHw")

(defdata :test/iam-access-entity-1
  :euuid #uuid "20000002-0000-0000-0000-000000000201" :xid "4xBqNdqb5afEvAmzkbMUNL")

(defdata :test/iam-access-entity-2
  :euuid #uuid "20000002-0000-0000-0000-000000000202" :xid "4xBqNdqb5afEvAmzkbMUNM")

(defdata :test/iam-access-relation-1
  :euuid #uuid "20000002-0000-0000-0000-000000000301" :xid "4xBqNdqb5afEvAmzkbMUSk")

(defdata :test/iam-access-relation-2
  :euuid #uuid "20000002-0000-0000-0000-000000000302" :xid "4xBqNdqb5afEvAmzkbMUSm")

(defdata :test/iam-access-scope-1
  :euuid #uuid "20000002-0000-0000-0000-000000000401" :xid "4xBqNdqb5afEvAmzkbMUXA")

(defdata :test/iam-access-scope-2
  :euuid #uuid "20000002-0000-0000-0000-000000000402" :xid "4xBqNdqb5afEvAmzkbMUXB")

(defdata :test/iam-access-user-2
  :euuid #uuid "20000002-0000-0000-0000-000000000002" :xid "4xBqNdqb5afEvAmzkbMUDX")

(defdata :test/iam-access-user-3
  :euuid #uuid "20000002-0000-0000-0000-000000000003" :xid "4xBqNdqb5afEvAmzkbMUDY")

(defdata :test/iam-access-role-3
  :euuid #uuid "20000002-0000-0000-0000-000000000103" :xid "4xBqNdqb5afEvAmzkbMUHx")

(defdata :test/iam-access-role-4
  :euuid #uuid "20000002-0000-0000-0000-000000000104" :xid "4xBqNdqb5afEvAmzkbMUHy")

(defdata :test/iam-access-role-5
  :euuid #uuid "20000002-0000-0000-0000-000000000105" :xid "4xBqNdqb5afEvAmzkbMUHz")

(defdata :test/iam-access-entity-3
  :euuid #uuid "20000002-0000-0000-0000-000000000203" :xid "4xBqNdqb5afEvAmzkbMUNN")

(defdata :test/iam-access-entity-4
  :euuid #uuid "20000002-0000-0000-0000-000000000204" :xid "4xBqNdqb5afEvAmzkbMUNP")

(defdata :test/iam-access-entity-5
  :euuid #uuid "20000002-0000-0000-0000-000000000205" :xid "4xBqNdqb5afEvAmzkbMUNQ")

(defdata :test/iam-access-entity-6
  :euuid #uuid "20000002-0000-0000-0000-000000000206" :xid "4xBqNdqb5afEvAmzkbMUNR")

;; General IAM Test (iam_test.clj)
(defdata :test/iam-user-1
  :euuid #uuid "20000003-0000-0000-0000-000000000001" :xid "4xBqNifnN3GvWaipg8usN8")

(defdata :test/iam-user-2
  :euuid #uuid "20000003-0000-0000-0000-000000000002" :xid "4xBqNifnN3GvWaipg8usN9")

(defdata :test/iam-user-3
  :euuid #uuid "20000003-0000-0000-0000-000000000003" :xid "4xBqNifnN3GvWaipg8usNA")

(defdata :test/iam-user-4
  :euuid #uuid "20000003-0000-0000-0000-000000000004" :xid "4xBqNifnN3GvWaipg8usNB")

(defdata :test/iam-user-5
  :euuid #uuid "20000003-0000-0000-0000-000000000005" :xid "4xBqNifnN3GvWaipg8usNC")

(defdata :test/iam-user-6
  :euuid #uuid "20000003-0000-0000-0000-000000000006" :xid "4xBqNifnN3GvWaipg8usND")

(defdata :test/iam-user-7
  :euuid #uuid "20000003-0000-0000-0000-000000000007" :xid "4xBqNifnN3GvWaipg8usNE")

(defdata :test/iam-user-8
  :euuid #uuid "20000003-0000-0000-0000-000000000008" :xid "4xBqNifnN3GvWaipg8usNF")

(defdata :test/iam-user-9
  :euuid #uuid "20000003-0000-0000-0000-000000000009" :xid "4xBqNifnN3GvWaipg8usNG")

(defdata :test/iam-user-10
  :euuid #uuid "20000003-0000-0000-0000-00000000000a" :xid "4xBqNifnN3GvWaipg8usNH")

(defdata :test/iam-role-1
  :euuid #uuid "20000003-0000-0000-0000-000000000101" :xid "4xBqNifnN3GvWaipg8usSY")

(defdata :test/iam-role-2
  :euuid #uuid "20000003-0000-0000-0000-000000000102" :xid "4xBqNifnN3GvWaipg8usSZ")

(defdata :test/iam-role-3
  :euuid #uuid "20000003-0000-0000-0000-000000000103" :xid "4xBqNifnN3GvWaipg8usSa")

(defdata :test/iam-group-1
  :euuid #uuid "20000003-0000-0000-0000-000000000201" :xid "4xBqNifnN3GvWaipg8usWx")

(defdata :test/iam-group-2
  :euuid #uuid "20000003-0000-0000-0000-000000000202" :xid "4xBqNifnN3GvWaipg8usWy")

(defdata :test/iam-client-1
  :euuid #uuid "20000003-0000-0000-0000-000000000301" :xid "4xBqNifnN3GvWaipg8usbN")

(defdata :test/iam-client-2
  :euuid #uuid "20000003-0000-0000-0000-000000000302" :xid "4xBqNifnN3GvWaipg8usbP")

(defdata :test/iam-client-3
  :euuid #uuid "20000003-0000-0000-0000-000000000303" :xid "4xBqNifnN3GvWaipg8usbQ")

(defdata :test/iam-user-11
  :euuid #uuid "20000003-0000-0000-0000-00000000000b" :xid "4xBqNifnN3GvWaipg8usNJ")

(defdata :test/iam-user-12
  :euuid #uuid "20000003-0000-0000-0000-00000000000c" :xid "4xBqNifnN3GvWaipg8usNK")

(defdata :test/iam-user-13
  :euuid #uuid "20000003-0000-0000-0000-00000000000d" :xid "4xBqNifnN3GvWaipg8usNL")

(defdata :test/iam-user-14
  :euuid #uuid "20000003-0000-0000-0000-00000000000e" :xid "4xBqNifnN3GvWaipg8usNM")

;;; ============================================================================
;;; Dataset Write Test Suite (write_test.clj)
;;; ============================================================================

(defdata :test/write-user-1
  :euuid #uuid "30000001-0000-0000-0000-000000000001" :xid "6vnFZHbVYxFzgvmqnJQuXW")

(defdata :test/write-user-2
  :euuid #uuid "30000001-0000-0000-0000-000000000002" :xid "6vnFZHbVYxFzgvmqnJQuXX")

(defdata :test/write-user-3
  :euuid #uuid "30000001-0000-0000-0000-000000000003" :xid "6vnFZHbVYxFzgvmqnJQuXY")

(defdata :test/write-user-4
  :euuid #uuid "30000001-0000-0000-0000-000000000004" :xid "6vnFZHbVYxFzgvmqnJQuXZ")

(defdata :test/write-user-5
  :euuid #uuid "30000001-0000-0000-0000-000000000005" :xid "6vnFZHbVYxFzgvmqnJQuXa")

(defdata :test/write-user-6
  :euuid #uuid "30000001-0000-0000-0000-000000000006" :xid "6vnFZHbVYxFzgvmqnJQuXb")

(defdata :test/write-user-7
  :euuid #uuid "30000001-0000-0000-0000-000000000007" :xid "6vnFZHbVYxFzgvmqnJQuXc")

(defdata :test/write-user-8
  :euuid #uuid "30000001-0000-0000-0000-000000000008" :xid "6vnFZHbVYxFzgvmqnJQuXd")

(defdata :test/write-user-9
  :euuid #uuid "30000001-0000-0000-0000-000000000009" :xid "6vnFZHbVYxFzgvmqnJQuXe")

(defdata :test/write-user-10
  :euuid #uuid "30000001-0000-0000-0000-00000000000a" :xid "6vnFZHbVYxFzgvmqnJQuXf")

(defdata :test/write-user-11
  :euuid #uuid "30000001-0000-0000-0000-00000000000b" :xid "6vnFZHbVYxFzgvmqnJQuXg")

(defdata :test/write-user-12
  :euuid #uuid "30000001-0000-0000-0000-00000000000c" :xid "6vnFZHbVYxFzgvmqnJQuXh")

(defdata :test/write-user-13
  :euuid #uuid "30000001-0000-0000-0000-00000000000d" :xid "6vnFZHbVYxFzgvmqnJQuXi")

(defdata :test/write-user-14
  :euuid #uuid "30000001-0000-0000-0000-00000000000e" :xid "6vnFZHbVYxFzgvmqnJQuXj")

(defdata :test/write-user-15
  :euuid #uuid "30000001-0000-0000-0000-00000000000f" :xid "6vnFZHbVYxFzgvmqnJQuXk")

(defdata :test/write-role-1
  :euuid #uuid "30000001-0000-0000-0000-000000000101" :xid "6vnFZHbVYxFzgvmqnJQubv")

(defdata :test/write-role-2
  :euuid #uuid "30000001-0000-0000-0000-000000000102" :xid "6vnFZHbVYxFzgvmqnJQubw")

(defdata :test/write-role-3
  :euuid #uuid "30000001-0000-0000-0000-000000000103" :xid "6vnFZHbVYxFzgvmqnJQubx")

;; Additional write-user and write-role declarations for dynamic tests
(defdata :test/write-user-16
  :euuid #uuid "30000001-0000-0000-0000-000000000010" :xid "6vnFZHbVYxFzgvmqnJQuXm")

(defdata :test/write-user-17
  :euuid #uuid "30000001-0000-0000-0000-000000000011" :xid "6vnFZHbVYxFzgvmqnJQuXn")

(defdata :test/write-user-18
  :euuid #uuid "30000001-0000-0000-0000-000000000012" :xid "6vnFZHbVYxFzgvmqnJQuXo")

(defdata :test/write-user-19
  :euuid #uuid "30000001-0000-0000-0000-000000000013" :xid "6vnFZHbVYxFzgvmqnJQuXp")

(defdata :test/write-user-20
  :euuid #uuid "30000001-0000-0000-0000-000000000014" :xid "6vnFZHbVYxFzgvmqnJQuXq")

(defdata :test/write-user-21
  :euuid #uuid "30000001-0000-0000-0000-000000000015" :xid "6vnFZHbVYxFzgvmqnJQuXr")

(defdata :test/write-user-22
  :euuid #uuid "30000001-0000-0000-0000-000000000016" :xid "6vnFZHbVYxFzgvmqnJQuXs")

(defdata :test/write-user-23
  :euuid #uuid "30000001-0000-0000-0000-000000000017" :xid "6vnFZHbVYxFzgvmqnJQuXt")

(defdata :test/write-user-24
  :euuid #uuid "30000001-0000-0000-0000-000000000018" :xid "6vnFZHbVYxFzgvmqnJQuXu")

(defdata :test/write-user-25
  :euuid #uuid "30000001-0000-0000-0000-000000000019" :xid "6vnFZHbVYxFzgvmqnJQuXv")

;; Additional roles for many-to-many tests (50+ roles needed)
(defdata :test/write-role-4
  :euuid #uuid "30000001-0000-0000-0000-000000000104" :xid "6vnFZHbVYxFzgvmqnJQuby")

(defdata :test/write-role-5
  :euuid #uuid "30000001-0000-0000-0000-000000000105" :xid "6vnFZHbVYxFzgvmqnJQubz")

(defdata :test/write-role-6
  :euuid #uuid "30000001-0000-0000-0000-000000000106" :xid "6vnFZHbVYxFzgvmqnJQuc1")

(defdata :test/write-role-7
  :euuid #uuid "30000001-0000-0000-0000-000000000107" :xid "6vnFZHbVYxFzgvmqnJQuc2")

(defdata :test/write-role-8
  :euuid #uuid "30000001-0000-0000-0000-000000000108" :xid "6vnFZHbVYxFzgvmqnJQuc3")

(defdata :test/write-role-9
  :euuid #uuid "30000001-0000-0000-0000-000000000109" :xid "6vnFZHbVYxFzgvmqnJQuc4")

(defdata :test/write-role-10
  :euuid #uuid "30000001-0000-0000-0000-00000000010a" :xid "6vnFZHbVYxFzgvmqnJQuc5")

(defdata :test/write-role-11
  :euuid #uuid "30000001-0000-0000-0000-00000000010b" :xid "6vnFZHbVYxFzgvmqnJQuc6")

(defdata :test/write-role-12
  :euuid #uuid "30000001-0000-0000-0000-00000000010c" :xid "6vnFZHbVYxFzgvmqnJQuc7")

(defdata :test/write-role-13
  :euuid #uuid "30000001-0000-0000-0000-00000000010d" :xid "6vnFZHbVYxFzgvmqnJQuc8")

(defdata :test/write-role-14
  :euuid #uuid "30000001-0000-0000-0000-00000000010e" :xid "6vnFZHbVYxFzgvmqnJQuc9")

(defdata :test/write-role-15
  :euuid #uuid "30000001-0000-0000-0000-00000000010f" :xid "6vnFZHbVYxFzgvmqnJQucA")

(defdata :test/write-role-16
  :euuid #uuid "30000001-0000-0000-0000-000000000110" :xid "6vnFZHbVYxFzgvmqnJQucB")

(defdata :test/write-role-17
  :euuid #uuid "30000001-0000-0000-0000-000000000111" :xid "6vnFZHbVYxFzgvmqnJQucC")

(defdata :test/write-role-18
  :euuid #uuid "30000001-0000-0000-0000-000000000112" :xid "6vnFZHbVYxFzgvmqnJQucD")

(defdata :test/write-role-19
  :euuid #uuid "30000001-0000-0000-0000-000000000113" :xid "6vnFZHbVYxFzgvmqnJQucE")

(defdata :test/write-role-20
  :euuid #uuid "30000001-0000-0000-0000-000000000114" :xid "6vnFZHbVYxFzgvmqnJQucF")

(defdata :test/write-role-21
  :euuid #uuid "30000001-0000-0000-0000-000000000115" :xid "6vnFZHbVYxFzgvmqnJQucG")

(defdata :test/write-role-22
  :euuid #uuid "30000001-0000-0000-0000-000000000116" :xid "6vnFZHbVYxFzgvmqnJQucH")

(defdata :test/write-role-23
  :euuid #uuid "30000001-0000-0000-0000-000000000117" :xid "6vnFZHbVYxFzgvmqnJQucJ")

(defdata :test/write-role-24
  :euuid #uuid "30000001-0000-0000-0000-000000000118" :xid "6vnFZHbVYxFzgvmqnJQucK")

(defdata :test/write-role-25
  :euuid #uuid "30000001-0000-0000-0000-000000000119" :xid "6vnFZHbVYxFzgvmqnJQucL")

(defdata :test/write-role-26
  :euuid #uuid "30000001-0000-0000-0000-00000000011a" :xid "6vnFZHbVYxFzgvmqnJQucM")

(defdata :test/write-role-27
  :euuid #uuid "30000001-0000-0000-0000-00000000011b" :xid "6vnFZHbVYxFzgvmqnJQucN")

(defdata :test/write-role-28
  :euuid #uuid "30000001-0000-0000-0000-00000000011c" :xid "6vnFZHbVYxFzgvmqnJQucP")

(defdata :test/write-role-29
  :euuid #uuid "30000001-0000-0000-0000-00000000011d" :xid "6vnFZHbVYxFzgvmqnJQucQ")

(defdata :test/write-role-30
  :euuid #uuid "30000001-0000-0000-0000-00000000011e" :xid "6vnFZHbVYxFzgvmqnJQucR")

(defdata :test/write-role-31
  :euuid #uuid "30000001-0000-0000-0000-00000000011f" :xid "6vnFZHbVYxFzgvmqnJQucS")

(defdata :test/write-role-32
  :euuid #uuid "30000001-0000-0000-0000-000000000120" :xid "6vnFZHbVYxFzgvmqnJQucT")

(defdata :test/write-role-33
  :euuid #uuid "30000001-0000-0000-0000-000000000121" :xid "6vnFZHbVYxFzgvmqnJQucU")

(defdata :test/write-role-34
  :euuid #uuid "30000001-0000-0000-0000-000000000122" :xid "6vnFZHbVYxFzgvmqnJQucV")

(defdata :test/write-role-35
  :euuid #uuid "30000001-0000-0000-0000-000000000123" :xid "6vnFZHbVYxFzgvmqnJQucW")

(defdata :test/write-role-36
  :euuid #uuid "30000001-0000-0000-0000-000000000124" :xid "6vnFZHbVYxFzgvmqnJQucX")

(defdata :test/write-role-37
  :euuid #uuid "30000001-0000-0000-0000-000000000125" :xid "6vnFZHbVYxFzgvmqnJQucY")

(defdata :test/write-role-38
  :euuid #uuid "30000001-0000-0000-0000-000000000126" :xid "6vnFZHbVYxFzgvmqnJQucZ")

(defdata :test/write-role-39
  :euuid #uuid "30000001-0000-0000-0000-000000000127" :xid "6vnFZHbVYxFzgvmqnJQuca")

(defdata :test/write-role-40
  :euuid #uuid "30000001-0000-0000-0000-000000000128" :xid "6vnFZHbVYxFzgvmqnJQucb")

(defdata :test/write-role-41
  :euuid #uuid "30000001-0000-0000-0000-000000000129" :xid "6vnFZHbVYxFzgvmqnJQucc")

(defdata :test/write-role-42
  :euuid #uuid "30000001-0000-0000-0000-00000000012a" :xid "6vnFZHbVYxFzgvmqnJQucd")

(defdata :test/write-role-43
  :euuid #uuid "30000001-0000-0000-0000-00000000012b" :xid "6vnFZHbVYxFzgvmqnJQuce")

(defdata :test/write-role-44
  :euuid #uuid "30000001-0000-0000-0000-00000000012c" :xid "6vnFZHbVYxFzgvmqnJQucf")

(defdata :test/write-role-45
  :euuid #uuid "30000001-0000-0000-0000-00000000012d" :xid "6vnFZHbVYxFzgvmqnJQucg")

(defdata :test/write-role-46
  :euuid #uuid "30000001-0000-0000-0000-00000000012e" :xid "6vnFZHbVYxFzgvmqnJQuch")

(defdata :test/write-role-47
  :euuid #uuid "30000001-0000-0000-0000-00000000012f" :xid "6vnFZHbVYxFzgvmqnJQuci")

(defdata :test/write-role-48
  :euuid #uuid "30000001-0000-0000-0000-000000000130" :xid "6vnFZHbVYxFzgvmqnJQucj")

(defdata :test/write-role-49
  :euuid #uuid "30000001-0000-0000-0000-000000000131" :xid "6vnFZHbVYxFzgvmqnJQuck")

(defdata :test/write-role-50
  :euuid #uuid "30000001-0000-0000-0000-000000000132" :xid "6vnFZHbVYxFzgvmqnJQucm")

;;; ============================================================================
;;; Dataset Delete Test Suite (delete_test.clj)
;;; ============================================================================

(defdata :test/delete-user-1
  :euuid #uuid "30000002-0000-0000-0000-000000000001" :xid "6vnFZNRgqQsgHLifhqyJg8")

(defdata :test/delete-user-2
  :euuid #uuid "30000002-0000-0000-0000-000000000002" :xid "6vnFZNRgqQsgHLifhqyJg9")

(defdata :test/delete-user-3
  :euuid #uuid "30000002-0000-0000-0000-000000000003" :xid "6vnFZNRgqQsgHLifhqyJgA")

(defdata :test/delete-user-4
  :euuid #uuid "30000002-0000-0000-0000-000000000004" :xid "6vnFZNRgqQsgHLifhqyJgB")

(defdata :test/delete-user-5
  :euuid #uuid "30000002-0000-0000-0000-000000000005" :xid "6vnFZNRgqQsgHLifhqyJgC")

(defdata :test/delete-user-6
  :euuid #uuid "30000002-0000-0000-0000-000000000006" :xid "6vnFZNRgqQsgHLifhqyJgD")

(defdata :test/delete-user-7
  :euuid #uuid "30000002-0000-0000-0000-000000000007" :xid "6vnFZNRgqQsgHLifhqyJgE")

(defdata :test/delete-user-8
  :euuid #uuid "30000002-0000-0000-0000-000000000008" :xid "6vnFZNRgqQsgHLifhqyJgF")

(defdata :test/delete-role-1
  :euuid #uuid "30000002-0000-0000-0000-000000000101" :xid "6vnFZNRgqQsgHLifhqyJkY")

(defdata :test/delete-role-2
  :euuid #uuid "30000002-0000-0000-0000-000000000102" :xid "6vnFZNRgqQsgHLifhqyJkZ")

(defdata :test/delete-nonexistent
  :euuid #uuid "30000002-0000-0000-0000-ffffffffffff" :xid "6vnFZNRgqQsgHNv9aKoDpz")

;;; ============================================================================
;;; Dataset Mapping Test Suite (euuid_mapping_test.clj)
;;; ============================================================================

(defdata :test/mapping-id-1
  :euuid #uuid "30000003-0000-0000-0000-000000000001" :xid "6vnFZTFt7sVMskfVdPXhpk")

(defdata :test/mapping-id-2
  :euuid #uuid "30000003-0000-0000-0000-000000000002" :xid "6vnFZTFt7sVMskfVdPXhpm")

(defdata :test/mapping-id-3
  :euuid #uuid "30000003-0000-0000-0000-000000000003" :xid "6vnFZTFt7sVMskfVdPXhpn")

(defdata :test/mapping-id-4
  :euuid #uuid "30000003-0000-0000-0000-000000000004" :xid "6vnFZTFt7sVMskfVdPXhpo")

(defdata :test/mapping-id-5
  :euuid #uuid "30000003-0000-0000-0000-000000000005" :xid "6vnFZTFt7sVMskfVdPXhpp")

(defdata :test/mapping-id-6
  :euuid #uuid "30000003-0000-0000-0000-000000000006" :xid "6vnFZTFt7sVMskfVdPXhpq")

(defdata :test/mapping-id-7
  :euuid #uuid "30000003-0000-0000-0000-000000000007" :xid "6vnFZTFt7sVMskfVdPXhpr")

(defdata :test/mapping-id-8
  :euuid #uuid "30000003-0000-0000-0000-000000000008" :xid "6vnFZTFt7sVMskfVdPXhps")

;;; ============================================================================
;;; Schema Corruption Test Suite (schema_corruption_test.clj)
;;; ============================================================================

;; Test dataset
(defdata :test/schema-corruption-dataset
  :euuid #uuid "ffffffff-0000-0000-0000-000000000000" :xid "YcVfxfaPoqpKFL8vKVKyCK")

;; Test versions
(defdata :test/schema-corruption-v1
  :euuid #uuid "ffffffff-0000-0000-0000-000000000001" :xid "YcVfxfaPoqpKFL8vKVKyCL")

(defdata :test/schema-corruption-v2
  :euuid #uuid "ffffffff-0000-0000-0000-000000000002" :xid "YcVfxfaPoqpKFL8vKVKyCM")

;; Test entities
(defdata :test/schema-corruption-entity-1
  :euuid #uuid "ffffffff-1111-0000-0000-000000000001" :xid "YcVfxfu5DBSmYkBTNt3Xxt")

(defdata :test/schema-corruption-entity-2
  :euuid #uuid "ffffffff-1111-0000-0000-000000000002" :xid "YcVfxfu5DBSmYkBTNt3Xxu")

;; Test entity attributes (must be stable to avoid "duplicate column" errors on re-deploy)
(defdata :test/schema-corruption-attr-1
  :euuid #uuid "ffffffff-2222-0000-0000-000000000001" :xid "YcVfxfu5DBSmYkBTNt3Xxv")

(defdata :test/schema-corruption-attr-2
  :euuid #uuid "ffffffff-2222-0000-0000-000000000002" :xid "YcVfxfu5DBSmYkBTNt3Xxw")

;;; ============================================================================
;;; Test Helper Common Data (test_helper.clj)
;;; ============================================================================

(defdata :test/helper-client
  :euuid #uuid "f0000001-0000-0000-0000-000000000001" :xid "WduFn6egcvqF4z5uDKpw2x")

(defdata :test/helper-user
  :euuid #uuid "f0000001-0000-0000-0000-000000000002" :xid "WduFn6egcvqF4z5uDKpw2y")

;;; ============================================================================
;;; Comment: Usage Examples
;;; ============================================================================

(comment
  ;; Load registrations
  (require '[synthigy.test-data])
  (require '[synthigy.dataset.id :as id])

  ;; With UUID provider (default)
  (id/data :test/human-grandma)
  ;; => #uuid "00000001-0000-0000-0000-000000000001"

  ;; With NanoID provider
  (id/set-provider! (id/->NanoIDProvider))
  (id/data :test/human-grandma)
  ;; => "_test_human_grandma_"

  ;; Reset to UUID provider
  (id/set-provider! (id/->UUIDProvider))

  ;; List all registered test data
  (id/registered-data))
