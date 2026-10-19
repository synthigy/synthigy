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

(ns synthigy.oauth.federated.oauth2
  "The `:oauth2` provider family — plain OAuth2 IdPs without an ID token."
  (:require
   [buddy.core.codecs :as codecs]
   [buddy.core.mac :as mac]
   [clojure.string :as str]
   [ring.util.codec :as codec]
   [synthigy.oauth.federated :as federated :refer [authorize-url fetch-identity]])
  (:import
   [java.net URI]
   [java.net.http HttpRequest HttpRequest$BodyPublishers]
   [java.time Duration]))

(defn http-get-bearer
  "Authenticated JSON GET; always sends a User-Agent (GitHub rejects requests without one)."
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
  ;; No nonce/PKCE: client_secret authenticates the exchange, `state` carries CSRF defense.
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
          ;; Hijack defense: require a PRIMARY + VERIFIED address, else no identity.
          emails (http-get-bearer (str (:userinfo-url cfg) "/emails") token)
          email  (some #(when (and (:primary %) (:verified %)) (:email %)) emails)]
      (when email
        {:iss    (or (:issuer cfg) "https://github.com")
         :sub    (str (:id user))    ; numeric id is stable; `login` is renameable
         :claims (assoc user :email email :email_verified true)}))))

(defmethod fetch-identity :facebook
  [cfg {:keys [code redirect-uri]}]
  ;; No email (phone-only signup) means no identity — fails closed.
  (when-let [token (oauth2-token cfg code redirect-uri)]
    (let [proof (codecs/bytes->hex (mac/hash token {:key (:client-secret cfg) :alg :hmac+sha256}))
          url   (:userinfo-url cfg)
          user  (http-get-bearer (str url (if (str/includes? url "?") "&" "?") "appsecret_proof=" proof) token)]
      (when-let [email (:email user)]
        {:iss    (or (:issuer cfg) "https://www.facebook.com")
         :sub    (str (:id user))
         :claims (assoc user :email email :email_verified true)}))))

(defmethod fetch-identity :discord
  [cfg {:keys [code redirect-uri]}]
  (when-let [token (oauth2-token cfg code redirect-uri)]
    (let [user (http-get-bearer (:userinfo-url cfg) token)]
      ;; Unverified addresses are hijackable — never make an identity from one.
      (when (and (:email user) (:verified user))
        {:iss    (or (:issuer cfg) "https://discord.com")
         :sub    (str (:id user))
         :claims (assoc user :email_verified true)}))))

(defmethod fetch-identity :oauth2 [cfg _]
  (throw (ex-info "OAuth2 fetch-identity not implemented for this provider"
                  {:provider (:provider cfg)})))
