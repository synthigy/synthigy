;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.federated.registry
  "Federation provider config; must never require synthigy.oauth.* — it breaks the federated require cycle."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.json :as json]
   [synthigy.log :as log]))

(id/defentity :id-federation/provider
  :euuid #uuid "5bc157c3-fa96-4ea2-9bf0-be2020b49bf8" :xid "CLAE8TtQwCKDKy5v6y5Qw1")

(id/defentity :id-federation/external-identity
  :euuid #uuid "174ad1a0-480a-48bb-970a-283d51cb9cd7" :xid "3spdYL3uzZXgTbJn9Rxd58")

(def ^:private provider-defaults
  {:google    {:discovery-url "https://accounts.google.com/.well-known/openid-configuration"
               :issuer        "https://accounts.google.com"
               :scopes        "openid email"}
   :microsoft {:scopes "openid email"}     ; discovery-url + issuer are tenant-specific
   :linkedin  {:discovery-url "https://www.linkedin.com/oauth/.well-known/openid-configuration"
               :issuer        "https://www.linkedin.com/oauth"
               :scopes        "openid profile email"}
   :github    {:authorize-url "https://github.com/login/oauth/authorize"
               :token-url     "https://github.com/login/oauth/access_token"
               :userinfo-url  "https://api.github.com/user"
               :scopes        "read:user user:email"}
   :facebook  {:authorize-url "https://www.facebook.com/v26.0/dialog/oauth"
               :token-url     "https://graph.facebook.com/v26.0/oauth/access_token"
               :userinfo-url  "https://graph.facebook.com/v26.0/me?fields=id,name,email"
               :scopes        "email public_profile"}
   :discord   {:authorize-url "https://discord.com/oauth2/authorize"
               :token-url     "https://discord.com/api/oauth2/token"
               :userinfo-url  "https://discord.com/api/users/@me"
               :scopes        "identify email"}})

(defn kebab-keys
  "Normalize snake_case JSON config keys to kebab (client_id -> :client-id)."
  [m]
  (update-keys m (fn [k] (keyword (str/replace (name k) "_" "-")))))

(defn provider-keyword
  "Enum column -> dispatch key, stripping the colon `str` keeps on a keyword enum."
  [provider]
  (keyword (str/lower-case (str/replace (str provider) #"^:" ""))))

;; :provider/:enabled merged LAST — a stray config key must never hijack dispatch.
(defn resolve-provider
  "Config of the one provider row for a dispatch type (e.g. \"google\"), or nil — fails closed."
  [provider-type]
  (try
    (let [enum-value (str/upper-case (name provider-type))
          ;; `{:_eq …}` explicitly — a bare value on an enum field throws.
          rows (dataset/search-entity
                :id-federation/provider
                {:provider {:_eq enum-value}}
                {:provider nil :active nil :configuration nil})]
      (when (next rows)
        (log/warn {:id ::duplicate-provider-rows :data {:provider enum-value :count (count rows)}}
                  "Multiple Federation Provider rows for one provider type — using the first"))
      (when-let [{:keys [provider active configuration]} (first rows)]
        (let [pkw    (provider-keyword provider)
              config (some-> configuration json/read-str kebab-keys)]
          (merge (provider-defaults pkw)
                 config
                 {:provider pkw :enabled (boolean active)}))))
    (catch Throwable e
      (log/warn {:id ::resolve-provider-failed :data {:provider provider-type :err (.getMessage e)}}
                "Could not load federation provider")
      nil)))

(def ^:private provider-labels
  {:google "Google" :microsoft "Microsoft" :github "GitHub" :facebook "Facebook"
   :linkedin "LinkedIn" :discord "Discord"
   :oidc_1 "SSO" :oidc_2 "SSO 2" :oidc_3 "SSO 3"})

(defn list-providers
  "Active providers as a public button list [{:name :provider :label :logo}]; never reads the encrypted configuration."
  []
  (try
    (->> (dataset/search-entity :id-federation/provider
                                {:active true}
                                {:provider nil :display_name nil :logo_url nil})
         (map (fn [{:keys [provider display_name logo_url]}]
                (let [pkw (provider-keyword provider)]
                  (cond-> {:name (clojure.core/name pkw) :provider pkw
                           :label (or (not-empty display_name)
                                      (provider-labels pkw)
                                      (str/capitalize (clojure.core/name pkw)))}
                    (not (str/blank? logo_url)) (assoc :logo logo_url)))))
         distinct
         vec)
    (catch Throwable e
      (log/warn {:id ::list-providers-failed :data {:err (.getMessage e)}}
                "Could not list federation providers")
      [])))
