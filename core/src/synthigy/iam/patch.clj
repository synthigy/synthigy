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

(ns synthigy.iam.patch
  "Version patches for the IAM component (separate from the IAM model); level
   via (patch/level! :synthigy/iam)."
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [synthigy.log :as log]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming :as naming]
    [synthigy.iam.access :refer [with-principal]]
    [synthigy.iam.keys]
    [synthigy.iam.transfer :as transfer]
    [synthigy.data :refer [*ROOT* *PUBLIC_ROLE*]]
    [synthigy.db :refer [*db*]]))

;; ============================================================================
;; Component Version Registration
;; ============================================================================

(patch/current-version :synthigy/iam "1.0.2")
;; ============================================================================
;; Version Patches
;; ============================================================================

(patch/upgrade :synthigy/iam "1.0.0"
               (log/info {:id ::initialized-1-0-0}
                         "IAM component initialized at v1.0.0"))

;; ============================================================================
;; 1.0.1 — person-info → user-public-profile backfill
;; ============================================================================

(def profile-claim-attrs
  "Claims moved from Person Info to User Public Profile in OAuth 0.1.8, as [person-info-column public-profile-key]."
  [[:name               :name]
   [:given_name         :given_name]
   [:family_name        :family_name]
   [:nickname           :nickname]
   [:preferred_username :preferred_username]
   [:profile            :profile]
   [:picture            :picture]
   [:website            :website]
   [:zoneinfo           :zone_info]])

(defn o2o-relation
  "The active User→`to-entity-id` o2o relation record from the deployed model."
  [to-entity-id]
  (let [model (dataset/deployed-model)]
    (some (fn [r]
            (when (and (:active r)
                       (= (id/extract (:from r)) (id/entity :iam/user))
                       (= (id/extract (:to r)) to-entity-id))
              r))
          (core/get-relations model))))

(defn query-pending
  "SQL for the users still needing a profile, given both link relations."
  [pi-rel pp-rel]
  (let [pi-link  (naming/relation->table-name pi-rel)
        pp-link  (naming/relation->table-name pp-rel)
        pi-fk    (naming/entity->relation-field (:to pi-rel))
        user-fk  (naming/entity->relation-field (:from pi-rel))
        cols     (str/join ", " (map (fn [[c _]] (str "pi.\"" (name c) "\"")) profile-claim-attrs))]
    (jdbc/execute!
     (:datasource *db*)
     [(format
       (str "SELECT u.\"%s\" AS user_id, u.name AS account_name, %s "
            "FROM person_info pi "
            "JOIN \"%s\" lp ON lp.\"%s\" = pi._eid "
            "JOIN \"user\" u ON u._eid = lp.\"%s\" "
            "LEFT JOIN \"%s\" lpp ON lpp.\"%s\" = u._eid "
            "WHERE lpp.\"%s\" IS NULL")
       (name (id/key)) cols pi-link pi-fk user-fk pp-link user-fk user-fk)]
     {:builder-fn rs/as-unqualified-lower-maps})))

(defn pending-profiles
  "Users with a person-info row but no public-profile row yet, with the moved
   claim values."
  []
  (let [pi-rel (o2o-relation (id/entity :iam/person-info))
        pp-rel (o2o-relation (id/entity :iam/user-public-profile))]
    (if (and pi-rel pp-rel)
      (query-pending pi-rel pp-rel)
      (do (log/info {:id ::public-profiles-not-deployed
                     :data {:action :skipped
                            :subject :user-public-profile
                            :person-info? (some? pi-rel)
                            :public-profile? (some? pp-rel)}}
                    "Skipping public profile backfill — relations not deployed")
          []))))

(defn profile-row
  "Project one SQL row onto a User write carrying a nested public profile, or
   nil when there's nothing to copy."
  [row]
  (let [copied (reduce (fn [acc [from to]]
                         (if-some [v (get row from)]
                           (assoc acc to v)
                           acc))
                       {}
                       profile-claim-attrs)]
    (when (seq copied)
      {(id/key) (:user_id row)
       :public_profile (assoc copied :name (or (:name copied) (:account_name row)))})))

(defn migrate-public-profiles!
  "Copy OIDC profile claims off person-info onto user-public-profile; idempotent, non-destructive."
  []
  (let [rows (pending-profiles)
        writes (vec (keep profile-row rows))]
    (doseq [w writes]
      (dataset/sync-entity (id/entity :iam/user) w))
    (log/info {:id ::public-profiles-migrated
               :data {:action :migrated
                      :subject :user-public-profile
                      :pending (count rows)
                      :written (count writes)}}
              "Backfilled user public profiles from person info")
    (count writes)))

(patch/upgrade :synthigy/iam "1.0.1"
               (migrate-public-profiles!))

(patch/upgrade :synthigy/iam "1.0.2"
               (with-principal nil
                 (transfer/import-role "exports/role_xsql_developer.json")
                 (doseq [role [*ROOT* *PUBLIC_ROLE*]]
                   (dataset/stack-entity :iam/user-role (select-keys role [(id/key) :description])))
                 (when-let [iam-user (dataset/get-entity :iam/user-role {:name "IAM User"} {(id/key) nil})]
                   (dataset/stack-entity :iam/user-role
                                         (assoc iam-user
                                                :active      false
                                                :description "Deprecated, not created on new installs. Read-only IAM view, fully covered by IAM Auditor.")))))
