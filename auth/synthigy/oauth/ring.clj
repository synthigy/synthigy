(ns synthigy.oauth.ring
  "Ring utilities for OAuth/OIDC

  This namespace provides utilities to replace Pedestal interceptor dependencies
  with lightweight Ring patterns. These utilities enable OAuth/OIDC handlers to
  work with any Ring-based framework (Compojure, Reitit, Luminus, Kit).

  Key features:
  - Middleware composition helpers
  - State passing through request map (replaces Pedestal context)
  - Zero Pedestal dependencies"
  (:require
   [clojure.walk :as walk]))

;; =============================================================================
;; Middleware Composition
;; =============================================================================

(defn compose-middleware
  "Compose middleware left-to-right, similar to Pedestal's (conj interceptors).

   Takes middleware functions and a final handler as arguments.
   The last argument is treated as the handler, all others are middleware.

   Example:
     (compose-middleware wrap-params wrap-cookies wrap-auth my-handler)

   Is equivalent to:
     (-> my-handler wrap-params wrap-cookies wrap-auth)

   This makes middleware composition more explicit and easier to test.

   If called with no arguments, returns identity function.
   If called with one argument, returns that argument unchanged."
  [& args]
  (if (empty? args)
    identity
    (let [handler (last args)
          middleware (butlast args)]
      (reduce (fn [h mw] (mw h))
              handler
              (reverse middleware)))))

;; =============================================================================
;; State Passing (Replaces Pedestal Context Map)
;; =============================================================================

(defn pass-state
  "Pass custom state through request map.

   Replaces Pedestal's pattern of adding custom keys to context:
     (assoc ctx ::session session-id)

   With Ring pattern:
     (pass-state request ::session session-id)

   Returns modified request map."
  [request k v]
  (assoc request k v))

(defn get-state
  "Get custom state from request map.

   Replaces Pedestal's pattern:
     (::session ctx)

   With Ring pattern:
     (get-state request ::session)

   Returns state value or nil."
  [request k]
  (get request k))

(defn update-state
  "Update custom state in request map with function.

   Example:
     (update-state request ::counter inc)

   Returns modified request map."
  [request k f & args]
  (apply update request k f args))

;; =============================================================================
;; Parameter Utilities
;; =============================================================================

(defn keywordize-params
  "Recursively keywordize all parameter keys in a map.

   Replaces Pedestal's keywordize-params interceptor."
  [params]
  (walk/keywordize-keys params))

(defn stringify-params
  "Recursively stringify all parameter keys in a map.

   Useful for outgoing requests where string keys are required."
  [params]
  (walk/stringify-keys params))

;; =============================================================================
;; Response Utilities
;; =============================================================================

(defn set-cookie
  "Add or update a cookie in the response.

   Example:
     (set-cookie response \"session\" \"abc123\" {:http-only true})

   Returns modified response map."
  ([response cookie-name value]
   (set-cookie response cookie-name value {}))
  ([response cookie-name value opts]
   (assoc-in response [:cookies cookie-name]
             (merge {:value value} opts))))

(defn delete-cookie
  "Delete a cookie by setting max-age to 0.

   Example:
     (delete-cookie response \"session\")

   Returns modified response map."
  [response cookie-name]
  (assoc-in response [:cookies cookie-name]
            {:value "" :max-age 0 :path "/"}))

(defn merge-cookies
  "Merge multiple cookie operations into response.

   Example:
     (merge-cookies response
       {\"session\" {:value \"abc\" :http-only true}
        \"prefs\" {:value \"xyz\" :max-age 3600}})

   Returns modified response map."
  [response cookies]
  (update response :cookies merge cookies))

;; =============================================================================
;; Early Return / Short-Circuit (Replaces chain/terminate)
;; =============================================================================

(defn short-circuit?
  "Check if middleware should short-circuit (not call downstream handler).

   In Ring, we short-circuit by returning a response directly instead of
   calling (handler request).

   This is conceptually equivalent to Pedestal's chain/terminate.

   Example:
     (if (authenticated? request)
       (handler request)          ; Continue chain
       {:status 401 :body \"Unauthorized\"})  ; Short-circuit

   This function is provided for documentation purposes and clarity."
  [response]
  (and (map? response)
       (contains? response :status)))

;; =============================================================================
;; CORS Support (Replaces Pedestal allow-origin)
;; =============================================================================

(defn wrap-cors
  "Add CORS headers to responses.

   Replaces Pedestal's allow-origin interceptor with Ring middleware.

   Options:
   - :allowed-origins - Set of allowed origin strings or :all for any origin
   - :allowed-methods - Set of allowed HTTP methods (default: #{:get :post :put :delete :options})
   - :allowed-headers - Set of allowed headers (default: #{\"*\"})
   - :max-age - Preflight cache duration in seconds (default: 3600)
   - :allow-credentials - Allow credentials (default: true)

   Example:
     (wrap-cors handler {:allowed-origins #{\"http://localhost:3000\"
                                            \"https://example.com\"}})

     (wrap-cors handler {:allowed-origins :all})"
  [handler {:keys [allowed-origins
                   allowed-methods
                   allowed-headers
                   max-age
                   allow-credentials]
            :or {allowed-methods #{:get :post :put :delete :options}
                 allowed-headers #{"*"}
                 max-age 3600
                 allow-credentials true}}]
  (fn [request]
    (let [origin (get-in request [:headers "origin"])
          origin-allowed? (cond
                            (= :all allowed-origins) true
                            (set? allowed-origins) (contains? allowed-origins origin)
                            (coll? allowed-origins) (some #(= % origin) allowed-origins)
                            :else false)]
      (if-not origin-allowed?
        ;; Origin not allowed - proceed without CORS headers
        (handler request)
        ;; Origin allowed - add CORS headers
        (if (= :options (:request-method request))
          ;; Handle preflight request
          {:status 200
           :headers {"Access-Control-Allow-Origin" origin
                     "Access-Control-Allow-Methods" (clojure.string/join ", " (map name allowed-methods))
                     "Access-Control-Allow-Headers" (if (set? allowed-headers)
                                                      (clojure.string/join ", " allowed-headers)
                                                      (first allowed-headers))
                     "Access-Control-Max-Age" (str max-age)
                     "Access-Control-Allow-Credentials" (str allow-credentials)}}
          ;; Normal request - add CORS headers to response
          (let [response (handler request)]
            (update response :headers merge
                    {"Access-Control-Allow-Origin" origin
                     "Access-Control-Allow-Credentials" (str allow-credentials)})))))))

(comment
  ;; Example usage

  ;; Compose middleware
  (def my-handler
    (compose-middleware
     wrap-params
     wrap-cookies
     wrap-keywordize
     my-oauth-handler))

  ;; Pass state through request
  (defn wrap-add-session [handler]
    (fn [request]
      (let [session-id (generate-session-id)]
        (handler (pass-state request ::session session-id)))))

  ;; Get state in handler
  (defn my-handler [request]
    (let [session-id (get-state request ::session)]
      {:status 200
       :body (str "Session: " session-id)}))

  ;; Set cookie in response
  (defn login-handler [request]
    (let [session-id (create-session)
          response {:status 302 :headers {"Location" "/"}}]
      (set-cookie response "idsrv.session" session-id
                  {:http-only true
                   :secure true
                   :same-site :none})))

  ;; Short-circuit pattern
  (defn wrap-auth [handler]
    (fn [request]
      (if (authenticated? request)
        (handler request)  ; Continue
        {:status 401 :body "Unauthorized"}))))  ; Short-circuit
