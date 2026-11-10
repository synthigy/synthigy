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

(ns synthigy.oauth.credentials
  "The owner of an account replaces its own password."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.iam :as iam]
   [synthigy.log :as log]
   [synthigy.oauth.onboarding :as onboarding]))

(def min-password-length 8)

(defn change-password!
  "Replace `user`'s password once the current one checks out, ending every session of the user; returns [status body]."
  [user current-password new-password]
  (cond
    (or (str/blank? current-password) (str/blank? new-password))
    [400 {:error "invalid_request"}]

    (str/blank? (:password user))
    [409 {:error "no_local_credential"}]

    (not (iam/validate-password current-password (:password user)))
    (do
      (log/warn {:id ::password-change-rejected
                 :data {:action :credentials-rejected :subject :user
                        :user (:name user)}}
                "Password change rejected — current password mismatch")
      [403 {:error "invalid_credentials"}])

    (< (count new-password) min-password-length)
    [422 {:error "password_too_short" :min_length min-password-length}]

    :else
    (do
      ;; plaintext — the dataset layer hashes `hashed` attributes on write; never hash here
      (dataset/sync-entity :iam/user {:xid (:xid user) :password new-password})
      (onboarding/kill-user-sessions! (:xid user))
      (log/info {:id ::password-changed
                 :data {:action :modified :subject :user-credential
                        :user (:name user)}}
                "Subject changed own password")
      [200 {:changed true}])))
