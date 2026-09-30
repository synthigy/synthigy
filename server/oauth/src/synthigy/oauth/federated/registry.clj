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

(ns synthigy.oauth.federated.registry
  "Federation provider CONFIG — the read side of the ID Federation dataset,
   sitting below every other federated namespace to break a require cycle. See
   docs/core/synthigy/oauth/federated/registry.md."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.json :as json]
   [synthigy.log :as log]))

;; =============================================================================
;; ID Federation entity references (pinned — deployed + stable)
;; =============================================================================

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
   :facebook  {:authorize-url "https://www.facebook.com/dialog/oauth"
               :token-url     "https://graph.facebook.com/oauth/access_token"
               :userinfo-url  "https://graph.facebook.com/me?fields=id,name,email"
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
  "Enum column -> dispatch key; the enum comes back as a keyword and `str` keeps
   the colon, so strip it before lower-casing."
  [provider]
  (keyword (str/lower-case (str/replace (str provider) #"^:" ""))))

;; :provider/:enabled are set from the columns LAST in the merge so a stray
;; config key can't hijack dispatch.
(defn resolve-provider
  "Load the one Federation Provider config for a dispatch type (e.g.
   \"google\"), or nil if unconfigured/unparseable (fails closed). Each
   provider type is a singleton slot — a model UNIQUE(provider) constraint
   enforces at most one row; if stale duplicates exist anyway, the first is
   used and the rest logged, never a 500 on the login page."
  [provider-type]
  (try
    (let [enum-value (str/upper-case (name provider-type))
          ;; `{:_eq …}` explicitly — a BARE value on an enum field throws
          ;; (ClassCastException in query-selection->sql), unlike string fields.
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
  "Active federation providers as a public button list [{:name :provider
   :label :logo}], one entry per provider type; never reads the encrypted
   configuration. `:name` is the dispatch type slug (the `?provider=` value);
   a row's display_name overrides the baked label, logo_url rides as :logo."
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
