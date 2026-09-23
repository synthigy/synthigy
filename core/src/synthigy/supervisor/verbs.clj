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

(ns synthigy.supervisor.verbs
  "Bootstrap, diagnosis and recovery verbs over the supervision channel."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.data :as data]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.log :as log]
    [synthigy.info :as info]
    [synthigy.supervisor :as supervisor]))

(defn require-started! [module]
  (when-not (lifecycle/started? module)
    (throw (ex-info (str (name module) " not started") {:code -32001}))))

(defn run-init!
  "Create tables, deploy schemas (lifecycle/setup!). Idempotent."
  []
  (log/info {:id ::init-start
             :data {:action :initialized :subject :admin}}
            "Running system initialization (lifecycle/setup!)")
  (lifecycle/setup! :synthigy/iam)
  {:modules [:synthigy/database :synthigy/dataset :synthigy/iam]})

(defn superusers []
  (require-started! :synthigy/dataset)
  (require-started! :synthigy/iam)
  (let [{users :users} (dataset/get-entity
                         :iam/user-role
                         {(id/key) (id/extract data/*ROOT*)}
                         {(id/key) nil
                          :users [{:selections {:name nil
                                                :active nil}}]})]
    (mapv :name users)))

(defn superuser-exists?
  "True when a user holds the ROOT role; false (never throws) otherwise."
  []
  (boolean
    (when (lifecycle/started? :synthigy/iam)
      (try (seq (superusers)) (catch Exception _ false)))))

(defn set-superuser! [username password]
  (when (or (str/blank? username) (str/blank? password))
    (throw (ex-info "Missing 'username' or 'password'" {:code -32602})))
  (require-started! :synthigy/iam)
  ;; stack, never sync — sync would REPLACE an existing user's role set
  (dataset/stack-entity :iam/user
                        {:name username
                         :password password
                         :active true
                         :roles [data/*ROOT*]})
  {:success true :message (str "Superuser '" username "' created/updated")})

(defn unset-superuser! [username]
  (when (str/blank? username)
    (throw (ex-info "Missing 'username'" {:code -32602})))
  (require-started! :synthigy/iam)
  (let [sus (superusers)]
    (when-not (some #{username} sus)
      (throw (ex-info (str "'" username "' is not a superuser") {:code -32602})))
    (when (= 1 (count sus))
      (throw (ex-info "Refusing to remove the last superuser" {:code -32000}))))
  (dataset/slice-entity :iam/user
    {:_where {:name {:_eq username}}}
    {:roles [{:args {:_where {(id/key) {:_eq (id/extract data/*ROOT*)}}}}]})
  {:success true :message (str "Superuser role removed from '" username "'")})

(defn by-name!
  [entity-id names label]
  (when (seq names)
    (let [wanted (set names)
          found (dataset/search-entity entity-id
                                       {:name {:_in (vec wanted)}}
                                       {(id/key) nil :name nil})
          missing (remove (set (map :name found)) wanted)]
      (when (seq missing)
        (throw (ex-info (str "Unknown " label "(s): " (str/join ", " missing))
                        {:code -32602})))
      (vec found))))

(defn add-client!
  "Create an OAuth client with role/API grants named rather than by id —
   no server, no OAuth round-trip. What a container build uses to seed a
   demo client (PLAN-SANDBOX-LOCAL.md's image/seed.sh) with no network."
  [{:keys [id name type secret settings roles apis]}]
  (require-started! :synthigy/iam)
  (when (str/blank? name)
    (throw (ex-info "Missing 'name'" {:code -32602})))
  (let [type (if (keyword? type) (clojure.core/name type) (str (or type "public")))]
    (when-not (#{"public" "confidential"} type)
      (throw (ex-info (str "Unknown type: " type) {:code -32602})))
    (when (and (or (seq roles) (seq apis)) (not= type "confidential"))
      (throw (ex-info "roles/apis require type=confidential" {:code -32602})))
    (let [roles (by-name! :iam/user-role roles "role")
          apis (by-name! :iam/api apis "API")
          client (iam/add-client (cond-> {:name name :type (keyword type)
                                          :secret secret :settings settings :apis apis}
                                   (not (str/blank? id)) (assoc :id id)))]
      (when (seq roles)
        (iam/set-user {:name (:id client) :type :SERVICE :active true :roles roles}))
      {:id (:id client) :secret (:secret client) :created (:created? client) :type type
       :roles (mapv :name roles) :apis (mapv :name apis)})))

(defn remove-client!
  "Delete an OAuth client and the SERVICE user `add-client!` created with it.
   Reports what was actually removed, so a teardown can be run twice."
  [{:keys [id]}]
  (require-started! :synthigy/iam)
  (when (str/blank? id)
    (throw (ex-info "Missing 'id'" {:code -32602})))
  (let [client (iam/get-client id)
        ;; The SERVICE user is named after the client id (add-client!'s own
        ;; companion write) — it has no relation back, so nothing cascades
        ;; and leaving it behind would block re-creating the same client.
        service (dataset/get-entity :iam/user {:name id :type :SERVICE} {(id/key) nil})]
    (when client (iam/remove-client client))
    (when service (dataset/delete-entity :iam/user service))
    {:id id :removed (boolean client) :service_user_removed (boolean service)}))

(defn public-active? []
  (require-started! :synthigy/iam)
  (let [role (dataset/get-entity :iam/user-role
                                 {(id/key) (id/extract data/*PUBLIC_ROLE*)}
                                 {(id/key) nil :active nil})
        user (dataset/get-entity :iam/user
                                 {(id/key) (id/extract data/*PUBLIC_USER*)}
                                 {(id/key) nil :active nil})]
    (boolean (and (:active role) (:active user)))))

(defn set-public! [active?]
  (require-started! :synthigy/iam)
  (let [v (boolean active?)]
    ;; stack, not sync — the __public__ user's role set must survive
    (dataset/stack-entity :iam/user-role
                          {(id/key) (id/extract data/*PUBLIC_ROLE*) :active v})
    (dataset/stack-entity :iam/user
                          {(id/key) (id/extract data/*PUBLIC_USER*) :active v})
    (log/warn {:id ::public-access-set
               :data {:action :modified :subject :iam-access :public v}}
              (str "Public (anonymous) access " (if v "ENABLED" "disabled")))
    {:success true :active v}))

(defn tools-client []
  (require-started! :synthigy/iam)
  (or (iam/get-client info/modeler-public-client-id)
      (throw (ex-info "Synthigy Tools client not found — initialize the system first"
                      {:code -32001}))))

(defn tools-info []
  (let [c (tools-client)]
    {:client_id info/modeler-public-client-id
     :redirections (get-in c [:settings "redirections"] [])
     :logout_redirections (get-in c [:settings "logout-redirections"] [])}))

(defn set-tools-redirections! [{:keys [redirections logout_redirections]}]
  (let [c (tools-client)
        clean (fn [urls]
                (vec (for [u urls
                           :let [u (str/trim (str u))]
                           :when (seq u)]
                       (if (re-matches #"https?://\S+" u)
                         u
                         (throw (ex-info (str "Invalid URL (must be http(s)://…): " u)
                                         {:code -32602}))))))]
    (dataset/stack-entity
     :iam/app
     {:id info/modeler-public-client-id
      :settings (cond-> (or (:settings c) {})
                  (some? redirections)
                  (assoc "redirections" (clean redirections))
                  (some? logout_redirections)
                  (assoc "logout-redirections" (clean logout_redirections)))})
    {:success true}))

(defn doctor-state
  "Health-check snapshot: module states, DB connectivity, encryption,
   ROOT_URL."
  []
  (let [dataset-started? (lifecycle/started? :synthigy/dataset)
        iam-started? (lifecycle/started? :synthigy/iam)
        encryption-started? (lifecycle/started? :synthigy.dataset/encryption)
        server-started? (lifecycle/started? :synthigy/server)

        enc-status (when encryption-started?
                     (try
                       (dataset-encryption/encryption-status)
                       (catch Exception _ nil)))

        db-connected (when (and dataset-started? iam-started?)
                       (try
                         (dataset/get-entity
                           :iam/user
                           {:name "nonexistent"}
                           {(id/key) nil})
                         true
                         (catch Exception _ false)))

        root-url (env :synthigy-iam-root-url)

        console-started? (when (lifecycle/module-info :synthigy/console)
                           (lifecycle/started? :synthigy/console))
        all-ok? (and dataset-started? iam-started? encryption-started?
                     server-started?
                     (not (false? console-started?))
                     (or (not encryption-started?) (:initialized enc-status))
                     (or (not db-connected) db-connected))
        ;; a boot in flight is not a broken instance — the console says
        ;; "starting", and only a module error makes it "degraded"
        busy? (and (not all-ok?)
                   (empty? @lifecycle/module-errors)
                   (supervisor/progress-running?))]
    (cond-> {:status (cond all-ok? "ok" busy? "starting" :else "degraded")
             ;; boot failures, for the operator console: encryption gets its
             ;; own field (drives the enter-master-key form), everything else
             ;; surfaces generically
             :encryption_error (some-> (get @lifecycle/module-errors :synthigy.dataset/encryption)
                                       :error supervisor/root-cause-message)
             :boot_error (some->> (vals @lifecycle/module-errors)
                                  first :error supervisor/root-cause-message)
             :checks {:dataset dataset-started?
                      ;; xid-native runtime on a legacy euuid layout — the
                      ;; operator console renders the migrate affordance off
                      ;; this. Backend-neutral: the provider IS the signal.
                      :legacy_euuid (= :euuid (id/key))
                      :iam iam-started?
                      :encryption encryption-started?
                      :encryption_initialized (get enc-status :initialized false)
                      :database (or db-connected false)
                      :server server-started?
                      :oauth (lifecycle/started? :synthigy/oauth.persistence)
                      :observability (when (and (not (supervisor/observability-disabled?))
                                                (lifecycle/module-info :synthigy/observability))
                                       (lifecycle/started? :synthigy/observability))
                      :console console-started?
                      :root_url (boolean root-url)}
             :progress (supervisor/progress)}
      (nil? root-url)
      (assoc :warnings
             [(str "SYNTHIGY_IAM_ROOT_URL is not set — OAuth redirect URIs, "
                   "issuer and login links are derived from request headers "
                   "(X-Forwarded-*), which is spoofable and wrong behind a "
                   "load balancer. Set it to the public origin before "
                   "production.")]))))

(defn database-stats
  "Backend, its server version, stored size and live pool occupancy."
  []
  (require-started! :synthigy/database)
  (let [database db/*db*
        ds       (:datasource database)
        backend  (str/lower-case (.getSimpleName (class database)))
        ;; Reflective on purpose: Hikari ships with the DATABASE module, not
        ;; with this one, so naming the class here would make the server
        ;; module refuse to compile without a backend on the classpath.
        pool     (try
                   (let [mx (.getHikariPoolMXBean ds)]
                     {:active  (.getActiveConnections mx)
                      :idle    (.getIdleConnections mx)
                      :total   (.getTotalConnections mx)
                      :waiting (.getThreadsAwaitingConnection mx)
                      :max     (.getMaximumPoolSize ds)})
                   (catch Throwable _ nil))
        sql      (if (= "sqlite" backend)
                   (str "select sqlite_version() as version, "
                        "(select page_count from pragma_page_count()) * "
                        "(select page_size from pragma_page_size()) as size")
                   "select version() as version, pg_database_size(current_database()) as size")
        row      (try
                   (with-open [c (jdbc/get-connection ds)]
                     (jdbc/execute-one! c [sql] {:builder-fn rs/as-unqualified-lower-maps}))
                   (catch Throwable _ nil))]
    (cond-> {:backend backend}
      pool           (assoc :pool pool)
      (:version row) (assoc :version (str/trim (str (:version row))))
      (:size row)    (assoc :size (:size row)))))

(defn run-initialize!
  "Create the first superuser. Throws ex-info on refusal, `:type` one of
   #{:validation-error :service-not-started :already-initialized
     :superuser-creation-failed}; every type's ex-data carries whatever
   :steps completed before the refusal.

   Dataset encryption is guaranteed initialized by the time this runs — a
   misconfigured/unresolvable provider fails boot loudly instead of leaving
   the system running sealed (see
   synthigy.dataset.encryption/ensure-initialized!), so there is no unseal
   step here."
  [username password]
  (let [steps (atom [])]
    (when (or (str/blank? username) (str/blank? password))
      (throw (ex-info "Missing superuser credentials: provide username and password"
                      {:type :validation-error})))

    (when-not (lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started :steps @steps})))

    (when (superuser-exists?)
      (swap! steps conj {:name "already-initialized"
                         :status "skipped"
                         :message "Superuser already exists"})
      (throw (ex-info "System already initialized"
                      {:type :already-initialized
                       :steps @steps})))

    (try
      (iam/set-user {:name username
                     :password password
                     :active true
                     :roles [data/*ROOT*]})
      (swap! steps conj {:name "superuser"
                         :status "complete"
                         :message (str "Superuser '" username "' created")})
      (catch Exception e
        (swap! steps conj {:name "superuser"
                           :status "failed"
                           :message (.getMessage e)})
        (throw (ex-info "Failed to create superuser"
                        {:type :superuser-creation-failed
                         :steps @steps}
                        e))))

    (let [db-check (try
                     (dataset/get-entity
                       :iam/user
                       {:name username}
                       {(id/key) nil})
                     true
                     (catch Exception _ false))
          enc-check (get (dataset-encryption/encryption-status) :initialized)]
      (if (and db-check enc-check)
        (swap! steps conj {:name "health"
                           :status "complete"
                           :message "All health checks passed"})
        (swap! steps conj {:name "health"
                           :status "warning"
                           :message (str "Health checks: database=" db-check
                                         ", encryption=" enc-check)})))

    {:steps @steps}))

(supervisor/register-method! "init"
  (fn [_]
    (assoc (run-init!) :success true :message "System initialized")))

(supervisor/register-method! "initialize"
  (fn [{:keys [username password]}]
    (try
      (let [{:keys [steps]} (run-initialize! username password)]
        {:success true :message "Initialization complete" :steps steps})
      (catch clojure.lang.ExceptionInfo e
        (if (= :already-initialized (:type (ex-data e)))
          {:success true
           :message "System already initialized"
           :steps (:steps (ex-data e))}
          (throw e))))))

(supervisor/register-method! "superuser.set"
  (fn [{:keys [username password]}]
    (set-superuser! username password)))

(supervisor/register-method! "superuser.unset"
  (fn [{:keys [username]}]
    (unset-superuser! username)))

(supervisor/register-method! "superuser.list"
  (fn [_] {:superusers (superusers)}))

(supervisor/register-method! "doctor"
  (fn [_] (doctor-state)))

(supervisor/register-method! "info"
  (fn [_]
    {:version "0.1.0"
     :encryption (dataset-encryption/encryption-status)
     :message "Synthigy supervisor"}))

(supervisor/register-method! "versions"
  (fn [_]
    {:components
     (vec (for [topic (sort (patch/registered-topics))
                :let [code (patch/version topic)
                      installed (try (patch/deployed-version topic) (catch Throwable _ nil))]
                :when code]
            ;; "0" is patcho's never-deployed marker — a component that
            ;; keeps no store version is untracked, not behind
            {:topic (str topic)
             :installed (when (not= "0" installed) installed)
             :code code
             :pending (boolean (and installed (not= "0" installed) (not= installed code)))}))}))

(supervisor/register-method! "encryption.status"
  (fn [_] (dataset-encryption/encryption-status)))

(supervisor/register-method! "encryption.rewrap"
  ;; Same-custody rewrap: unwrap+wrap every DEK through the CURRENT provider.
  ;; Vault's encrypt always uses the transit key's latest version, so this is
  ;; how stored wrappers catch up after a rotation inside Vault — no config
  ;; change, no restart. encryption.migrate cannot do this: its tag-collision
  ;; guard refuses a target whose tag is already on a row.
  (fn [_]
    (when-not (:initialized (dataset-encryption/encryption-status))
      (throw (ex-info "Encryption not initialized" {:code -32001})))
    (let [{:keys [migrated-count provider]} (dataset-encryption/rewrap-deks!)]
      {:success true :migrated migrated-count :provider provider})))

(supervisor/register-method! "iam.add-client"
  (fn [params] (add-client! params)))

;; The teardown half. A demo that seeds a client must be able to hand the
;; instance back the way it found it, and over the supervision channel the
;; loopback bind IS the credential — no token to obtain on an instance that
;; has just been created.
(supervisor/register-method! "iam.remove-client"
  (fn [params] (remove-client! params)))

(supervisor/register-method! "db.stats"
  (fn [_] (database-stats)))

(supervisor/register-method! "public.status"
  (fn [_] {:active (public-active?)}))

(supervisor/register-method! "public.set"
  (fn [{:keys [active]}] (set-public! active)))

(supervisor/register-method! "key.rotate"
  ;; Rewrap every DEK under a fresh generated LOCAL master key and hand the
  ;; key back to the DAEMON, which owns persisting it (.env) — the operator
  ;; never handles key material. This is also the federated->local RETURN
  ;; path: migrate-local-key! resolves each row's own wrap_provider (vault/
  ;; webhook/local, whichever is active) and rewraps under the new local
  ;; key, so rotating while already local and migrating BACK from a
  ;; federated provider are the same operation. It never touches the
  ;; federated provider's own root key.
  (fn [_]
    (when-not (:initialized (dataset-encryption/encryption-status))
      (throw (ex-info "Encryption not initialized" {:code -32001})))
    (let [k (dataset-encryption/random-master)
          {:keys [migrated-count]} (dataset-encryption/migrate-local-key! k)]
      {:success true :master_key k :migrated migrated-count})))

(defn custody-provider
  "KeyWrapProvider for a custody target built from RPC params, not this
   process's env — the daemon writes .env and only restarts afterwards, so
   the new config exists nowhere else yet. Shared by encryption.probe and
   encryption.migrate so a config that tests green cannot be interpreted
   differently by the migration that follows it."
  [{:keys [target addr transit_key transit_mount token role mount jwt_path
           url secret timeout_ms]}]
  (case target
    "vault"   (do (when (or (str/blank? (str addr)) (str/blank? (str transit_key)))
                    (throw (ex-info "Vault custody needs both addr and transit_key"
                                    {:code -32602})))
                  ;; the whole map reaches vault-token, which reads :token
                  ;; (static) or falls back to Kubernetes auth via
                  ;; :role/:mount/:jwt-path — drop them and every migration
                  ;; tries k8s auth and fails on a laptop
                  (dataset-encryption/->VaultTransitKeyWrapProvider
                    {:addr addr :transit-key transit_key
                     :transit-mount transit_mount :timeout-ms timeout_ms
                     :token token :role role :mount mount
                     :jwt-path jwt_path}))
    "webhook" (do (when (str/blank? (str url))
                    (throw (ex-info "Webhook custody needs url" {:code -32602})))
                  (dataset-encryption/->WebhookKeyWrapProvider
                    {:url url :secret secret :timeout-ms timeout_ms}))
    (throw (ex-info (str "Unsupported custody target " (pr-str target)
                         " — vault and webhook are the federated targets; "
                         "moving back to a local master key is key.rotate")
                    {:code -32602}))))

(supervisor/register-method! "encryption.probe"
  ;; Wrap/unwrap a throwaway 32 bytes through a candidate provider and
  ;; check they survive the round trip. Constructed LOCALLY and never
  ;; registered, so a failed probe leaves no trace in the provider registry
  ;; and no DEK is touched — the operator console can offer Test before a
  ;; migration commits, the same way db.probe/obs.probe do before launch.
  (fn [params]
    (try
      (let [p     (custody-provider params)
            bytes (byte-array 32)
            _     (.nextBytes (java.security.SecureRandom.) bytes)
            back  (dataset-encryption/unwrap-dek p (dataset-encryption/wrap-dek p bytes))]
        (if (java.util.Arrays/equals ^bytes bytes ^bytes back)
          {:ok true :message "Round-trip OK — the provider wraps and unwraps correctly."}
          {:ok false :message "The provider answered, but the unwrapped key did not match — refusing."}))
      ;; Only the PARAM validation above is a protocol error (-32602, the
      ;; console renders it as a refusal). A provider that answers badly —
      ;; wrong token, denied path, 5xx — is a probe VERDICT, not a broken
      ;; request, and its status/body is the most useful thing an operator
      ;; can be told, so it must not escape as a bare RPC error.
      (catch clojure.lang.ExceptionInfo e
        (if (= -32602 (:code (ex-data e)))
          (throw e)
          (let [{:keys [status body]} (ex-data e)]
            {:ok false :message (cond-> (str "Provider refused: " (ex-message e))
                                  status (str " (HTTP " status ")")
                                  (not-empty (str body))
                                  (str " — " (subs (str body) 0 (min 200 (count (str body))))))})))
      (catch Throwable e
        {:ok false :message (str "Could not reach the provider: "
                                 (or (not-empty (ex-message e)) (.getSimpleName (class e))))}))))

(supervisor/register-method! "encryption.migrate"
  ;; Rewrap every DEK under a DIFFERENT custody provider. Local
  ;; master-key rotation is key.rotate's job, not this one: two local
  ;; providers both tag "default" and migrate-provider! refuses that.
  (fn [params]
    (when-not (:initialized (dataset-encryption/encryption-status))
      (throw (ex-info "Encryption not initialized" {:code -32001})))
    (let [{:keys [migrated-count provider]}
          (dataset-encryption/migrate-provider! (custody-provider params))]
      {:success true :migrated migrated-count :provider provider})))

(supervisor/register-method! "tools.info"
  (fn [_] (tools-info)))

(supervisor/register-method! "tools.redirections.set"
  (fn [params] (set-tools-redirections! params)))

(supervisor/register-method! "progress"
  (fn [_] (supervisor/progress)))
