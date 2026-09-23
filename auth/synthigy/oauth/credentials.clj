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
   [synthigy.oauth.core :as core]
   [synthigy.oauth.onboarding :as onboarding]))

(def min-password-length 8)

(defn json-response [status body]
  {:status status :headers {"Content-Type" "application/json"} :body (json/write-str body)})

(defn token-subject
  "The user behind this request's bearer token, with the stored credential, or
   nil. Reads under no principal — the credential column is superuser-only."
  [request]
  (when-let [token (some->> (get-in request [:headers "authorization"])
                            (re-find #"(?i)bearer\s+(.+)")
                            second)]
    (when-let [{:keys [sub xid]} (encryption/unsign-data token)]   ; verifies signature + exp
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
          user (token-subject request)]
      (cond
        (nil? user)
        (json-response 401 {:error "unauthorized"})

        (not (:active user))
        (json-response 403 {:error "account_inactive"})

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
