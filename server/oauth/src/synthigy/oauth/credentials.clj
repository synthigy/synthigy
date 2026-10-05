;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.credentials
  "Subject-driven credential change — the authenticated owner of an account
   replaces its own password. See docs/core/synthigy/oauth/credentials.md."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.authentication :as authentication]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.onboarding :as onboarding]))

(def min-password-length 8)

(defn json-response [status body]
  {:status status :headers {"Content-Type" "application/json"} :body (json/write-str body)})

(defn token-subject
  "The user behind an unexpired bearer token, with the stored credential, or
   nil. Reads under no principal — the credential column is superuser-only."
  [token]
  (when (authentication/validate-token token)
    (let [{:keys [sub xid]} (encryption/unsign-data token)]
      (when-let [args (cond xid {(id/key) xid} sub {:name sub})]
        (dataset/get-entity :iam/user
                            args
                            {:xid nil :name nil :active nil :password nil})))))

(defn change-password-handler
  "POST /oauth/password {current_password, new_password} — the subject, and only
   the subject, replaces its own credential. Every live session dies with the
   old password, this one included."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [current_password new_password]} (core/parse-body request)
          token (authentication/extract-bearer-token request)
          user  (token-subject token)]
      (cond
        (nil? token)
        {:status 401 :headers {"WWW-Authenticate" "Bearer"}}

        (not (:active user))
        (assoc-in (json-response 401 {:error "invalid_token"})
                  [:headers "WWW-Authenticate"] "Bearer error=\"invalid_token\"")

        (or (str/blank? current_password) (str/blank? new_password))
        (json-response 400 {:error "invalid_request"})

        ;; An account with no local credential has nothing to prove against —
        ;; it claims one through a ticket, never here.
        (str/blank? (:password user))
        (json-response 409 {:error "no_local_credential"})

        (not (iam/validate-password current_password (:password user)))
        (do
          (log/warn {:id ::password-change-rejected
                     :data {:action :credentials-rejected :subject :user
                            :user (:name user)}}
                    "Password change rejected — current password mismatch")
          (json-response 403 {:error "invalid_credentials"}))

        (< (count new_password) min-password-length)
        (json-response 422 {:error "password_too_short"
                            :min_length min-password-length})

        :else
        (do
          ;; plaintext — the dataset layer derives `hashed` attributes on write
          (dataset/sync-entity :iam/user {:xid (:xid user) :password new_password})
          (onboarding/kill-user-sessions! (:xid user))
          (log/info {:id ::password-changed
                     :data {:action :modified :subject :user-credential
                            :user (:name user)}}
                    "Subject changed own password")
          (json-response 200 {:changed true}))))))
