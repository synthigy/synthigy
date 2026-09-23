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

(ns synthigy.oauth.federated.oauth2
  "The `:oauth2` provider family — plain OAuth2 (no ID token) IdPs;
   `fetch-identity` is written per provider. See
   docs/core/synthigy/oauth/federated/oauth2.md."
  (:require
   [ring.util.codec :as codec]
   [synthigy.oauth.federated :as federated :refer [authorize-url fetch-identity]])
  (:import
   [java.net URI]
   [java.net.http HttpRequest HttpRequest$BodyPublishers]
   [java.time Duration]))

(defn http-get-bearer
  "Authenticated JSON GET for OAuth2 userinfo; always sends a User-Agent (GitHub
   rejects requests without one)."
  [url token]
  (federated/send-json
   (-> (HttpRequest/newBuilder (URI/create url))
       (.timeout (Duration/ofSeconds 5))
       (.header "Accept" "application/json")
       (.header "User-Agent" "synthigy")
       (.header "Authorization" (str "Bearer " token))
       .GET
       .build)))

(defmethod authorize-url :oauth2
  [cfg {:keys [redirect-uri state]}]
  ;; No nonce/PKCE: plain OAuth2 has no ID token; client_secret authenticates
  ;; the exchange, `state` carries CSRF defense.
  (str (:authorize-url cfg) "?"
       (codec/form-encode {:client_id (:client-id cfg)
                           :redirect_uri redirect-uri
                           :scope (or (:scopes cfg) "read:user user:email")
                           :state state
                           :response_type "code"})))

(defn oauth2-token
  "Exchange an OAuth2 authorization code for an access-token string, or nil."
  [cfg code redirect-uri]
  (:access_token
   (federated/send-json
    (-> (HttpRequest/newBuilder (URI/create (:token-url cfg)))
        (.timeout (Duration/ofSeconds 5))
        (.header "Content-Type" "application/x-www-form-urlencoded")
        (.header "Accept" "application/json")
        (.POST (HttpRequest$BodyPublishers/ofString
                (codec/form-encode {:grant_type "authorization_code"
                                    :code code
                                    :redirect_uri redirect-uri
                                    :client_id (:client-id cfg)
                                    :client_secret (:client-secret cfg)})))
        .build))))

(defmethod fetch-identity :github
  [cfg {:keys [code redirect-uri]}]
  (when-let [token (oauth2-token cfg code redirect-uri)]
    (let [user   (http-get-bearer (:userinfo-url cfg) token)
          ;; Hijack defense: GitHub email is private/unverified by default —
          ;; require a PRIMARY + VERIFIED address from /user/emails or no
          ;; identity.
          emails (http-get-bearer (str (:userinfo-url cfg) "/emails") token)
          email  (some #(when (and (:primary %) (:verified %)) (:email %)) emails)]
      (when email
        {:iss    (or (:issuer cfg) "https://github.com")
         :sub    (str (:id user))    ; numeric id is stable; `login` is renameable
         :claims (assoc user :email email :email_verified true)}))))

(defmethod fetch-identity :facebook
  [cfg {:keys [code redirect-uri]}]
  ;; Graph /me only returns email when the account has one and granted the
  ;; scope — phone-only signups get none, and no email means no identity to
  ;; onboard (fails closed, same posture as GitHub).
  (when-let [token (oauth2-token cfg code redirect-uri)]
    (let [user (http-get-bearer (:userinfo-url cfg) token)]
      (when-let [email (:email user)]
        {:iss    (or (:issuer cfg) "https://www.facebook.com")
         :sub    (str (:id user))
         :claims (assoc user :email email :email_verified true)}))))

(defmethod fetch-identity :discord
  [cfg {:keys [code redirect-uri]}]
  (when-let [token (oauth2-token cfg code redirect-uri)]
    (let [user (http-get-bearer (:userinfo-url cfg) token)]
      ;; :verified is Discord's own email-verification flag — unverified
      ;; addresses are hijackable, so they don't make an identity.
      (when (and (:email user) (:verified user))
        {:iss    (or (:issuer cfg) "https://discord.com")
         :sub    (str (:id user))
         :claims (assoc user :email_verified true)}))))

(defmethod fetch-identity :oauth2 [cfg _]
  (throw (ex-info "OAuth2 fetch-identity not implemented for this provider"
                  {:provider (:provider cfg)})))
