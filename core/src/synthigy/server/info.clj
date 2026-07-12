(ns synthigy.server.info
  "Public server discovery — answers 'is auth required, and if so, where?'
   without requiring any auth itself.

   The modeling web component fetches `/.well-known/synthigy` on init before
   triggering any OAuth flow. Servers running without `:synthigy/iam` advertise
   `{:auth {:required false}}` and the modeler skips its login UI entirely.

   Stays a leaf namespace: depends only on patcho.lifecycle at the require
   level. No IAM, no dataset, no audit — must remain callable in every
   deployment shape. The optional deploy-version block is late-bound via
   `requiring-resolve` (only when :synthigy/dataset is running) precisely to
   preserve that leaf property."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.json :as json]))

(def ^:private modeler-public-client-id
  "SYNTHIGYCOMPONENTSPUBLICCLIENTFOROAUTHFLOWPOPUPWIN")

(defn- model-info
  "Deploy drift-stamp for the codegen client — `{:version, :version-id,
   :deployed-at}` (kebab-case, JSON-serialized verbatim) — present only when
   `:synthigy/dataset` is running. Late-bound via `requiring-resolve` so this
   namespace keeps no static dataset dependency and stays callable in
   deployments that have no dataset."
  []
  (when (lifecycle/started? :synthigy/dataset)
    ((requiring-resolve 'synthigy.dataset/deployed-version-info))))

(defn- discovery-body []
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        model       (model-info)]
    (cond-> {:service "synthigy"
             :auth {:required iam-active?}}
      iam-active?
      (assoc-in [:auth :oidc]
                {:discovery "/.well-known/openid-configuration"
                 :client_id modeler-public-client-id})

      model
      (assoc :model model))))

(defn handler
  "Public discovery endpoint. Returns whether the server requires auth and,
   when it does, enough OIDC bootstrapping info for the modeler to start a
   login flow. Cacheable for a short window since lifecycle changes are rare
   but possible (operator restarts with IAM enabled/disabled)."
  [_request]
  {:status 200
   :headers {"Content-Type" "application/json"
             "Cache-Control" "max-age=60"
             "Access-Control-Allow-Origin" "*"}
   :body (json/write-str (discovery-body))})
