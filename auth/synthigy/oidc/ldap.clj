(ns synthigy.oidc.ldap
  "LDAP authentication integration for Synthigy IAM.

  This namespace provides LDAP authentication support. It is optional and will
  gracefully degrade if LDAP is not configured or the clj-ldap library is not available."
  (:require
    [clj-ldap.client :as ldap]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]))

(defonce ^:dynamic *pool* nil)

(defn start
  "Initialize LDAP connection pool from environment variables.

  Required environment variables:
  - LDAP_SERVER: LDAP server hostname
  - LDAP_BIND_DN: Bind DN for authentication
  - LDAP_PASSWORD: Password for bind DN

  Optional environment variables:
  - LDAP_PORT: LDAP server port (default: 389)
  - LDAP_SSL: Enable SSL (default: false)
  - LDAP_TLS: Enable STARTTLS (default: false)"
  []
  (try
    (let [ldap-config {:host (str
                               (env :ldap-server)
                               (when-some [port (env :ldap-port)]
                                 (str ":" port)))
                       :bind-dn (env :ldap-bind-dn)
                       :ssl? (env :ldap-ssl)
                       :startTLS? (when-some [tls (env :ldap-tls)]
                                    (if (#{"true" "TRUE" "yes" "YES" "1"} tls)
                                      true
                                      false))
                       :password (env :ldap-password)}]
      (when (and (env :ldap-server) (env :ldap-bind-dn))
        (when-some [pool (ldap/connect ldap-config)]
          (log/info "[OIDC LDAP] Successfully initialized LDAP connection pool")
          (alter-var-root #'*pool* (fn [_] pool)))))
    (catch java.io.FileNotFoundException _
      (log/debug "[OIDC LDAP] clj-ldap library not available - LDAP authentication disabled"))
    (catch Throwable ex
      (log/warnf ex "[OIDC LDAP] Failed to initialize LDAP pool")))
  *pool*)

(defn validate-credentials
  "Validate user credentials against LDAP server.

  Returns true if credentials are valid, false otherwise.
  If LDAP is not configured or unavailable, returns false."
  ([data] (validate-credentials *pool* data))
  ([pool {:keys [username password]}]
   (when pool
     (try
       (ldap/bind? pool username password)
       (catch Throwable ex
         (log/debugf ex "[OIDC LDAP] Failed to validate credentials for user: %s" username)
         false)))))

(comment
  (start)
  (validate-credentials
    *pool*
    {:username "test"
     :password "test"}))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/ldap
  {:depends-on [:synthigy/iam]
   :start (fn []
            ;; Runtime: Initialize LDAP connection pool (optional)
            (log/info "[LDAP] Starting LDAP authentication...")
            (start)
            (if *pool*
              (log/info "[LDAP] LDAP authentication started")
              (log/info "[LDAP] LDAP not configured - skipping")))})
