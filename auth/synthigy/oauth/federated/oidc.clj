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

(ns synthigy.oauth.federated.oidc
  "The `:oidc` provider family — discovery, JWKS, and ID-token validation shared
   by every OIDC-discovery-compliant IdP. See
   docs/core/synthigy/oauth/federated/oidc.md."
  (:require
   [buddy.core.keys :as keys]
   [buddy.sign.jws :as jws]
   [buddy.sign.jwt :as jwt]
   [clojure.core.cache :as cache]
   [ring.util.codec :as codec]
   [synthigy.log :as log]
   [synthigy.oauth.federated :as federated :refer [authorize-url fetch-identity]])
  (:import
   [java.net URI]
   [java.net.http HttpRequest HttpRequest$BodyPublishers]
   [java.time Duration]))

;; =============================================================================
;; Discovery + JWKS (cached, 1h TTL)
;; =============================================================================

(def ^:private ttl-ms (* 60 60 1000))
(defonce ^:private discovery-cache
  (atom (cache/ttl-cache-factory {} :ttl ttl-ms)))

(defn cached
  "Read-through TTL cache for discovery docs + JWKS; the double-miss race is
   benign since the fetch is idempotent."
  [url]
  (let [c @discovery-cache]
    (if (cache/has? c url)
      (do (swap! discovery-cache cache/hit url)
          (cache/lookup c url))
      (let [v (federated/http-json url)]
        (swap! discovery-cache assoc url v)
        v))))

(defn discovery [cfg] (cached (:discovery-url cfg)))
(defn jwks-keys [jwks-uri] (:keys (cached jwks-uri)))

;; =============================================================================
;; ID token validation
;; =============================================================================

(defn public-key-for [keys-vec kid]
  (some-> (first (filter #(= kid (:kid %)) keys-vec))
          keys/jwk->public-key))

;; Pins RS256 (alg-confusion defense); resolves the signing key by `kid` from
;; the provider JWKS; nonce is matched against the value bound into our state.
(defn verify-id-token
  [disc id-token aud issuer nonce]
  (try
    (let [kid (:kid (jws/decode-header id-token))
          pub (public-key-for (jwks-keys (:jwks_uri disc)) kid)]
      (when pub
        (let [claims (jwt/unsign id-token pub {:alg :rs256 :iss issuer :aud aud})]
          (when (= nonce (:nonce claims)) claims))))
    (catch Throwable e
      (log/warn {:id ::id-token-rejected :data {:err (.getMessage e)}}
                "Upstream ID token rejected")
      nil)))

(defn exchange-code [cfg disc code redirect-uri verifier]
  (federated/send-json
   (-> (HttpRequest/newBuilder (URI/create (:token_endpoint disc)))
       (.timeout (Duration/ofSeconds 5))
       (.header "Content-Type" "application/x-www-form-urlencoded")
       (.header "Accept" "application/json")
       (.POST (HttpRequest$BodyPublishers/ofString
               (codec/form-encode {:grant_type "authorization_code"
                                   :code code
                                   :redirect_uri redirect-uri
                                   :client_id (:client-id cfg)
                                   :client_secret (:client-secret cfg)
                                   :code_verifier verifier})))
       .build)))

(defmethod authorize-url :oidc
  [cfg {:keys [redirect-uri state nonce challenge prompt]}]
  (let [disc (discovery cfg)]
    (str (:authorization_endpoint disc) "?"
         (codec/form-encode
          (cond-> {:client_id (:client-id cfg)
                   :response_type "code"
                   :redirect_uri redirect-uri
                   :scope (or (:scopes cfg) "openid email")
                   :state state
                   :nonce nonce
                   :code_challenge challenge
                   :code_challenge_method "S256"}
            prompt (assoc :prompt prompt))))))

(defmethod fetch-identity :oidc
  [cfg {:keys [code redirect-uri verifier nonce iss]}]
  (let [disc   (discovery cfg)
        issuer (or (:issuer cfg) (:issuer disc))]
    (when (or (nil? iss) (= iss issuer))            ; RFC 9207 mix-up defense
      (let [tokens (exchange-code cfg disc code redirect-uri verifier)
            claims (verify-id-token disc (:id_token tokens) (:client-id cfg) issuer nonce)]
        (when claims
          {:iss issuer :sub (:sub claims) :claims claims})))))
