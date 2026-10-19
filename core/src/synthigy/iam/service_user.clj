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

(ns synthigy.iam.service-user
  "The SERVICE user a confidential OAuth client authenticates as, linked
   through the OAuth Client `service_user` relation."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]))

(defn confidential? [{:keys [type]}]
  (boolean (#{:confidential "confidential"} type)))

(defn client-credentials? [{:keys [settings] :as client}]
  (boolean (and (confidential? client)
                (some #{"client_credentials"} (get settings "allowed-grants")))))

(defn user-selection [] {(id/key) nil :name nil :active nil})

(defn get-service-user
  [client-key]
  (:service_user
   (dataset/get-entity :iam/app {(id/key) client-key}
                       {:service_user [{:selections (user-selection)}]})))

(def service-user-lock (Object.))

(defn service-user-name [{:keys [id name]}]
  (if (str/blank? name) id name))

(defn assert-name-free!
  "Throw :service-user-name-taken when a user other than `user-key` holds `user-name`."
  [user-name user-key]
  (when-let [other (dataset/get-entity :iam/user {:name user-name} {(id/key) nil})]
    (when (not= (id/extract other) user-key)
      (throw (ex-info (str "A user named \"" user-name "\" already exists")
                      {:code :service-user-name-taken
                       :name user-name
                       :user (id/extract other)})))))

(defn get-client
  [client-key]
  (dataset/get-entity :iam/app {(id/key) client-key}
                      {(id/key) nil :id nil :name nil :type nil :settings nil
                       :service_user [{:selections (user-selection)}]}))

(defn sync-service-user
  "Keep a confidential client's SERVICE user linked, named after the client,
   and active exactly while the client may use client_credentials."
  [client-key]
  (locking service-user-lock
    (when-let [{:keys [service_user] :as client} (get-client client-key)]
      (let [user-name (service-user-name client)
            live? (client-credentials? client)]
        (cond
          service_user
          (if (and (= user-name (:name service_user))
                   (= live? (boolean (:active service_user))))
            service_user
            (do (assert-name-free! user-name (id/extract service_user))
                (dataset/stack-entity :iam/user {(id/key) (id/extract service_user)
                                                 :name user-name :active live?})))

          (confidential? client)
          (do (assert-name-free! user-name nil)
              (let [user (dataset/stack-entity :iam/user {:name user-name :type :SERVICE :active live?})]
                (dataset/stack-entity :iam/app {(id/key) client-key
                                                :service_user {(id/key) (id/extract user)}})
                user)))))))

(defn adopt-service-user
  "Link an unlinked client to the SERVICE user named after its client id."
  [client-key]
  (locking service-user-lock
    (when-let [{:keys [id service_user]} (get-client client-key)]
      (when-not service_user
        (when-let [legacy (dataset/get-entity :iam/user {:name id}
                                              {(id/key) nil :type nil
                                               :authorized_client [{:selections {(id/key) nil}}]})]
          (when (and (#{:SERVICE "SERVICE"} (:type legacy)) (nil? (:authorized_client legacy)))
            (dataset/stack-entity :iam/app {(id/key) client-key
                                            :service_user {(id/key) (id/extract legacy)}})
            (dissoc legacy :authorized_client)))))))
