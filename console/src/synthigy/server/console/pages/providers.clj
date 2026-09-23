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

(ns synthigy.server.console.pages.providers
  (:require
   [clojure.string :as str]
   [synthigy.oauth.core :as oauth.core]
   [synthigy.oauth.federated :as federated]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.xsql.console :as cx]))

(def provider-slots
  "The supported slots, in display order — one row each, at most, by UNIQUE(provider)."
  [["GOOGLE" "Google"]
   ["MICROSOFT" "Microsoft"]
   ["GITHUB" "GitHub"]
   ["FACEBOOK" "Facebook"]
   ["LINKEDIN" "LinkedIn"]
   ["DISCORD" "Discord"]
   ["OIDC_1" "Custom OIDC 1"]
   ["OIDC_2" "Custom OIDC 2"]
   ["OIDC_3" "Custom OIDC 3"]])

(defn check-provider
  [provider-type]
  (try
    (let [cfg (registry/resolve-provider provider-type)]
      (cond
        (nil? cfg)
        [:warn "Could not load this provider's configuration."]

        (str/blank? (str (:discovery-url cfg)))
        [:warn "This provider has no discovery document to check."]

        :else
        (let [disc     (federated/http-json (:discovery-url cfg))
              found    (:issuer disc)
              expected (:issuer cfg)]
          (cond
            (str/blank? (str found))
            [:warn "Discovery document loaded, but it carries no issuer."]

            (and (not (str/blank? (str expected))) (not= expected found))
            [:warn (str "Issuer mismatch — discovery says " found
                        ", this configuration says " expected ".")]

            :else
            [:ok (str "Discovery OK — issuer " found "."
                      (when (str/blank? (str (:client-id cfg)))
                        " Note: Client ID is not set yet."))]))))
    (catch Exception e
      [:warn (str "Discovery fetch failed: " (ex-message e))])))

(def oidc-family #{"GOOGLE" "MICROSOFT" "LINKEDIN" "OIDC_1" "OIDC_2" "OIDC_3"})

(defn wizard-callback
  "The redirect URI as the federated flow itself will build it for this host."
  [request]
  (binding [oauth.core/*domain* (oauth.core/original-uri request)]
    (federated/callback-uri)))

(def setup-guides
  {"GOOGLE"
   {:where "Google Cloud Console → APIs & Services → Credentials"
    :url "https://console.cloud.google.com/apis/credentials"
    :stages
    [{:label "Set up"
      :callback? true
      :steps
      ["Pick the project this sign-in belongs to, or create one."
       (str "Configure the OAuth consent screen if you haven't already. While it is "
            "unpublished only the accounts you list as test users can sign in.")
       "Create credentials → OAuth client ID, and choose application type Web application."
       (str "Under \"Authorised redirect URIs\", Add URI and paste the redirect URI below "
            "exactly — no trailing slash, and the scheme and port must match.")
       (str "Create — the dialog that appears carries the Client ID and Client secret "
            "the next step asks for.")]}]
    :note (str "Google's endpoints are built in, so the client ID and secret are all "
               "this needs.")}

   "MICROSOFT"
   {:where "Microsoft Entra admin center → App registrations"
    :url "https://entra.microsoft.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade"
    :stages
    [{:label "Register"
      :callback? true
      :steps
      ["New registration, and give it a name your users will recognise on the consent screen."
       (str "Under \"Supported account types\", choose \"My organization only\" — "
            "Synthigy supports one Microsoft tenant per install; a multi-tenant or "
            "personal-account registration's discovery URL won't resolve to a single "
            "tenant, which this setup doesn't support.")
       (str "Still in the registration form, set Redirect URI to platform Web with the "
            "URI below as its value, then Register. Already registered without one? "
            "Authentication in the left nav → Add a platform → Web, and add it there.")]}]
    :note (str "One Microsoft tenant per Synthigy install — this is the tenant your "
               "users sign into, not a list to add to. Leave Issuer blank unless you "
               "need to pin it; Synthigy falls back to the issuer the discovery "
               "document itself declares.")}

   "GITHUB"
   {:where "GitHub → Settings → Developer settings → OAuth Apps"
    :url "https://github.com/settings/developers"
    :stages
    [{:label "Create app"
      :callback? true
      :steps
      ["New OAuth App, and give it a name and homepage URL."
       "Set \"Authorization callback URL\" to the redirect URI below, exactly."
       "Register application."]}]
    :note (str "GitHub is plain OAuth 2.0, not OpenID Connect, so there is no discovery "
               "document to verify against. This provider is created disabled — enable "
               "it once you have tested a sign-in.")}

   "FACEBOOK"
   {:where "Meta for Developers → My Apps"
    :url "https://developers.facebook.com/apps/"
    :stages
    [{:label "Create app"
      :callback? true
      :steps
      [(str "Create App, and pick the use case that includes Facebook Login for the "
            "web — add the Facebook Login product if the use case didn't already.")
       (str "Facebook Login → Settings → \"Valid OAuth Redirect URIs\" — paste the URI "
            "below exactly, and save.")]}]
    :note (str "Plain OAuth 2.0 — no discovery document, so this is created disabled; "
               "test a sign-in, then enable. While the app is in development mode only "
               "its listed testers can sign in. Accounts without an email address "
               "(phone-only signups) cannot onboard.")}

   "LINKEDIN"
   {:where "LinkedIn Developers → My Apps"
    :url "https://www.linkedin.com/developers/apps"
    :stages
    [{:label "Create app"
      :callback? true
      :steps
      ["Create app — it must be associated with a LinkedIn Page."
       (str "On the Products tab, request \"Sign In with LinkedIn using OpenID "
            "Connect\" — the auth endpoints don't work without it.")
       (str "On the Auth tab under \"OAuth 2.0 settings\", add the redirect URI "
            "below to Authorized redirect URLs, exactly.")]}]
    :note (str "LinkedIn's OpenID Connect endpoints are built in, so the client ID and "
               "secret are all this needs.")}

   "DISCORD"
   {:where "Discord Developer Portal → Applications"
    :url "https://discord.com/developers/applications"
    :stages
    [{:label "Create app"
      :callback? true
      :steps
      ["New Application, and name it what your users should see on the consent screen."
       "On the OAuth2 page, add the redirect URI below under Redirects, exactly."]}]
    :note (str "Plain OAuth 2.0 — no discovery document, so this is created disabled; "
               "test a sign-in, then enable. Only Discord accounts with a verified "
               "email can onboard.")}})

(def oidc-guide
  "One guide for all three custom slots — any OpenID Connect-compliant IdP."
  {:where "Your identity provider's admin console (Okta, Auth0, Keycloak, Entra, …)"
   :stages
   [{:label "Register client"
     :callback? true
     :steps
     [(str "Create an OIDC client — a confidential \"web application\" using the "
           "authorization code flow — and allow it the openid, profile and email "
           "scopes.")
      "Register the redirect URI below with that client, exactly."]}]
   :note (str "Works with any OpenID Connect-compliant provider. After creating, set a "
              "Display name (and optionally a logo URL) on the provider's page — that "
              "is what the sign-in button shows instead of the slot name.")})

(def guides
  (into setup-guides (map (fn [v] [v oidc-guide])) ["OIDC_1" "OIDC_2" "OIDC_3"]))

(def oidc-layout
  [[:client-id "Client ID" :text
    "Copy the client ID from the client you registered into this field." nil]
   [:client-secret "Client secret" :password
    "Copy that client's secret into this field." nil]
   [:discovery-url "Discovery URL" :text
    (str "Copy the issuer's discovery URL into this field — usually "
         "<issuer>/.well-known/openid-configuration.")
    {:placeholder "https://idp.example.com/.well-known/openid-configuration"}]
   [:issuer "Issuer" :text
    (str "Optional — blank uses the issuer the discovery document declares. "
         "Set it only to pin one.")
    nil]
   [:scopes "Scopes" :text "Space-separated."
    {:placeholder "openid profile email"}]])

(defn wizard-finish
  "Verify the freshly created provider and enable it when its issuer checks out."
  [spec {xid :xid provider :provider}]
  (let [[kind enable? msg]
        (if-not (contains? oidc-family (some-> provider name))
          [:ok false (str "Provider created. There is no discovery document to verify "
                          "for this one, so it stays disabled — test a sign-in, then "
                          "switch it on below.")]
          (let [[verdict m] (check-provider provider)]
            (if (= :ok verdict)
              [:ok true (str m " Provider enabled — it now renders on the sign-in pages.")]
              [:warn false (str "Created, but left disabled: " m)])))]
    (if (data/set-active! spec xid enable?)
      [kind msg]
      [:warn (str msg " (Could not write the Active flag — set it below.)")])))

(defn provider-badge
  [row]
  (let [dv (some-> (:provider row) name)]
    (widgets/badge
     {:brand? true
      :glyph (get assets/brand-icons (some-> dv str/lower-case keyword)
                  assets/generic-brand-icon)
      :label (or (second (get widgets/type-glyphs dv)) dv "—")})))

(defn check-panel
  [row notice]
  (when (contains? oidc-family (some-> (:provider row) name))
    (widgets/tool-panel
     {:action (str "/console/iam/providers/" (:xid row) "/check")
      :heading "Check discovery"
      :hint (str "Fetches the provider's OpenID discovery document and confirms "
                 "the issuer matches this configuration — catches endpoint typos "
                 "without a full sign-in round-trip.")
      :notice notice
      :submit "Check"})))

(defn run-check
  [_spec row _params]
  {:panel-notice (check-provider (:provider row))})

(def spec
  {:slug "providers" :entity :federation_provider :key :id-federation/provider
   :label "Identity Providers" :icon :globe
   :table {:query cx/providers-table
           :watch cx/watch-providers-table
           :sortable #{"provider" "active"}}
   :subtitle (str "Upstream identity providers for federated sign-in — one slot per "
                  "provider (Microsoft is a single tenant). An active row renders as "
                  "a \"Continue with …\" button on the login pages.")
   :columns [[:provider "Provider" :brand] [:active "Status" :status]]
   :cards {:by :provider :slots provider-slots :badge provider-badge}
   :actions {"check" {:run run-check}}
   :create {:hint (str "Pick a provider, register Synthigy with it, then paste back "
                       "the credentials it gives you — the steps in between depend on "
                       "which one you choose. Each provider is a single slot: creating "
                       "a second one for the same provider replaces which row Synthigy "
                       "uses to sign people in.")
            :wizard {:guides guides
                     :submit "Create provider"
                     :callback wizard-callback}
            :after wizard-finish
            :fields [[:provider "Provider" :choices
                      {:required true :keyword? true :choices provider-slots}]]}
   :delete {:warning (str "Anyone who signed in through it keeps their account, "
                          "but loses that sign-in method and needs another way in.")}
   :detail {:fields [[:active "Active" :switch]
                     [:display_name "Display name" :text]
                     [:logo_url "Logo URL" :text]]
            :panels [["Check discovery" check-panel]]
            :config {:attr :configuration :string? true :by :provider
                     :badge provider-badge
                     :heading "Provider configuration"
                     :layouts
                     {"GOOGLE"
                      [[:client-id "Client ID" :text
                        (str "Copy the Client ID from the credentials dialog (or "
                             "reopen the client from the Credentials list) into "
                             "this field.")
                        nil]
                       [:client-secret "Client secret" :password
                        (str "Copy the Client secret into this field — it can be "
                             "re-shown or rotated from the Credentials list.")
                        nil]
                       [:scopes "Scopes" :text "Space-separated."
                        {:placeholder "openid email"}]]
                      "MICROSOFT"
                      [[:client-id "Client ID" :text
                        (str "Copy the Application (client) ID from Overview → "
                             "Essentials into this field.")
                        nil]
                       [:client-secret "Client secret" :password
                        (str "Overview → Client credentials → New client secret — "
                             "copy the VALUE column (not the Secret ID) into this "
                             "field; it is shown once and never again.")
                        nil]
                       [:discovery-url "Discovery URL" :text
                        (str "Overview → Endpoints — copy the \"OpenID Connect "
                             "metadata document\" URL into this field, already "
                             "built for your tenant.")
                        {:placeholder "https://login.microsoftonline.com/<tenant>/v2.0/.well-known/openid-configuration"}]
                       [:issuer "Issuer" :text
                        (str "Optional — blank uses the issuer the discovery "
                             "document declares. Set it only to pin one.")
                        {:placeholder "https://login.microsoftonline.com/<tenant>/v2.0"}]
                       [:scopes "Scopes" :text "Space-separated."
                        {:placeholder "openid email"}]]
                      "GITHUB"
                      [[:client-id "Client ID" :text
                        "Copy the Client ID from the OAuth App's page into this field."
                        nil]
                       [:client-secret "Client secret" :password
                        (str "Generate a new client secret there and copy it into "
                             "this field — GitHub only shows it once.")
                        nil]
                       [:scopes "Scopes" :text "Space-separated."
                        {:placeholder "read:user user:email"}]]
                      "FACEBOOK"
                      [[:client-id "Client ID" :text
                        (str "App settings → Basic — copy the App ID into this "
                             "field.")
                        nil]
                       [:client-secret "Client secret" :password
                        "Show and copy the App secret into this field."
                        nil]
                       [:scopes "Scopes" :text "Space-separated."
                        {:placeholder "email public_profile"}]]
                      "LINKEDIN"
                      [[:client-id "Client ID" :text
                        (str "Copy the Client ID from the app's Auth tab into this "
                             "field.")
                        nil]
                       [:client-secret "Client secret" :password
                        (str "Copy the Primary Client Secret from the same tab "
                             "into this field.")
                        nil]
                       [:scopes "Scopes" :text "Space-separated."
                        {:placeholder "openid profile email"}]]
                      "DISCORD"
                      [[:client-id "Client ID" :text
                        (str "Copy the Client ID from the application's OAuth2 "
                             "page into this field.")
                        nil]
                       [:client-secret "Client secret" :password
                        (str "Reset Secret there and copy the value into this "
                             "field.")
                        nil]
                       [:scopes "Scopes" :text "Space-separated."
                        {:placeholder "identify email"}]]
                      "OIDC_1" oidc-layout
                      "OIDC_2" oidc-layout
                      "OIDC_3" oidc-layout}}}})
