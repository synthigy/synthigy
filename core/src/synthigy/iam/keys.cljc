(ns synthigy.iam.keys
  "Well-known IAM entity/data identity registrations.

   Shared by backend and frontend: both classpaths resolve `synthigy.iam.keys`
   so there is a single source of truth for these ids. Requiring this
   namespace registers provider-agnostic lookups via `synthigy.dataset.id`:

     (require '[synthigy.dataset.id :as id] '[synthigy.iam.keys])
     (id/entity :iam/user)        ;; => euuid or xid, depending on active provider
     (id/data   :iam.model/version-0.80.0)

   Registrations happen at namespace load time. Add new IAM entities/data
   here rather than duplicating `defentity`/`defdata` forms elsewhere."
  (:require
   #?(:clj  [synthigy.dataset.id :refer        [defentity defdata]]
      :cljs [synthigy.dataset.id :refer-macros [defentity defdata]])))

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

(defentity :iam/project
  :euuid #uuid "342f54bd-5a1b-4024-8b64-566862f82627" :xid "7Skf8MvReE4XsEQXdtHXWn")

;; ============================================================================
;; IAM Data
;; ============================================================================

(defdata :iam/id
  :euuid #uuid "c5c85417-0aef-4c44-9e86-8090647d6378" :xid "RRY5JcXr8MwppjvK9bhp7Z")

(defdata :iam.model/version-0.80.0
  :euuid #uuid "7c9981ed-8494-47b0-9580-adc2951819f9" :xid "GPPpgwJqAuSEbRRuoMYLLg")
