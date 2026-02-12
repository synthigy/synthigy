(ns synthigy.iam.context
  "Pluggable user context provider pattern.

  Provides a unified, extensible way to retrieve user context (user data, roles,
  groups) with caching and database fallback. Supports OAuth, scripts, tests,
  and future HTTP middleware.

  Architecture based on patcho's VersionStore pattern:
  - Protocol-based pluggable providers
  - Global provider management with scoped overrides
  - Default cached implementation with DB fallback
  - Simple high-level API for common usage

  Usage:

    ;; Script/REPL usage
    (require '[synthigy.iam.context :as ctx])

    ;; Bind context and execute
    (ctx/with-user-context \"admin\"
      (dataset/sync-entity ...))

    ;; Get context without binding
    (let [user-ctx (ctx/get-user-context admin-id)]
      (:roles user-ctx))

    ;; Test usage
    (let [test-provider (ctx/->AtomUserContextProvider
                          (atom {test-id test-user-data}))]
      (ctx/with-user-context-provider test-provider
        (ctx/with-user-context test-id
          ;; Test code runs with bound context
          (is (= expected (my-function))))))

    ;; Custom provider with lifecycle
    (defrecord LDAPUserProvider [config ^:unsynchronized-mutable connection]
      ctx/UserContextProvider
      (lookup-user [_ username]
        (ldap/query-user connection username))
      (invalidate-user [_ _] nil)
      (clear-cache [_] nil)
      (start! [this]
        (set! connection (ldap/connect config))
        nil)
      (stop! [this]
        (when connection
          (ldap/disconnect connection)
          (set! connection nil))
        nil))

    ;; Lifecycle is automatic - stop! called on old provider, start! on new
    (ctx/set-user-context-provider! (->LDAPUserProvider ldap-config nil))"
  (:require
    [clojure.core.async :as async]
    [clojure.tools.logging :as log]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.iam.access :as access]))

;; ============================================================================
;; Protocol
;; ============================================================================

(defprotocol UserContextProvider
  "Protocol for retrieving user context (user data, roles, groups).

  Implementations can provide custom user lookup strategies (LDAP,
  microservices, external auth systems, etc.) while maintaining a
  consistent interface for the rest of the application."

  (lookup-user [this identifier]
    "Look up user context by identifier.

    Args:
      identifier - Can be:
                   - ID (UUID or XID string - user's entity ID)
                   - String (username)
                   - Integer (user's :_eid)

    Returns:
      User context map with keys:
        {:_eid      Integer     ; Database entity ID
         :<id-key>  ID          ; User entity ID (euuid or xid)
         :name      String      ; Username
         :password  String      ; Password hash
         :active    Boolean     ; Active status
         :avatar    String      ; Avatar URL
         :settings  Map         ; User settings
         :roles     Set<ID>     ; Role IDs
         :groups    Set<ID>}    ; Group IDs

      Returns nil if user not found.")

  (invalidate-user [this identifier]
    "Invalidate cached user context for the given identifier.

    Args:
      identifier - Same types as lookup-user

    Returns:
      nil")

  (clear-cache [this]
    "Clear all cached user contexts.

    Returns:
      nil")

  (start! [this]
    "Start the provider lifecycle (cache invalidation, connections, etc.).

    Called automatically by set-user-context-provider!

    Returns:
      nil")

  (stop! [this]
    "Stop the provider and cleanup resources (listeners, connections, etc.).

    Called automatically when setting a new provider.

    Returns:
      nil"))

;; ============================================================================
;; Global Provider Management
;; ============================================================================

(defonce ^{:dynamic true
           :doc "The current user context provider.

  Can be temporarily overridden with with-user-context-provider macro.
  Set the default provider with set-user-context-provider!

  See UserContextProvider protocol for implementation details."}
  *user-context-provider*
  nil)

(defn set-user-context-provider!
  "Set the default user context provider.

  This updates the root binding of *user-context-provider* and will be
  used by all threads unless temporarily overridden with
  with-user-context-provider.

  Automatically stops the old provider and starts the new one.

  Args:
    provider - Implementation of UserContextProvider protocol

  Returns:
    nil"
  [provider]
  ;; Stop old provider if exists
  (when-let [old-provider *user-context-provider*]
    (try
      (stop! old-provider)
      (catch Exception e
        (log/warn e "[UserContext] Error stopping old provider"))))

  ;; Set new provider
  (alter-var-root #'*user-context-provider* (constantly provider))

  ;; Start new provider
  (try
    (start! provider)
    (catch Exception e
      (log/error e "[UserContext] Error starting new provider")
      (throw e)))

  nil)

(defmacro with-user-context-provider
  "Temporarily override the user context provider for the scope of body.

  Useful for testing or when different parts of the application need
  different provider strategies.

  Args:
    provider - Implementation of UserContextProvider protocol
    body     - Expressions to evaluate with the overridden provider

  Returns:
    Result of evaluating body"
  [provider & body]
  `(binding [*user-context-provider* ~provider]
     ~@body))

(comment
  (dataset/search-entity
    :iam/user
    nil
    {(id/key) nil
     :name nil}))

(defn get-user-details [args]
  (some->
    (dataset/get-entity
      :iam/user
      args
      {:_eid nil
       (id/key) nil
       :name nil
       :password nil
       :active nil
       :avatar nil
       :settings nil
       :person_info [{:selections
                      {:name nil
                       :given_name nil
                       :middle_name nil
                       :nickname nil
                       :prefered_username nil
                       :profile nil
                       :picture nil
                       :website nil
                       :email nil
                       :email_verified nil
                       :gender nil
                       :birth_date nil
                       :zoneinfo nil
                       :phone_number nil
                       :phone_number_verified nil
                       :address nil}}]
       :groups [{:selections {(id/key) nil}}]
       :roles [{:selections {(id/key) nil}}]})
    (update :roles #(set (map id/extract %)))
    (update :groups #(set (map id/extract %)))))

;; ============================================================================
;; Default Implementation: Cached Provider with DB Fallback
;; ============================================================================

(defn ->CachedUserContextProvider
  "Create a cached user context provider with automatic cache invalidation.

  Uses lexical closure to maintain cache and channels. The go-loop monitors
  user entity changes and invalidates cache entries automatically."
  []
  (let [cache (atom {})
        delta-chan (async/chan (async/sliding-buffer 100))
        stop-chan (async/chan)]
    (reify UserContextProvider

      (lookup-user [_ identifier]
        (let [;; Try to resolve identifier to a native ID
              user-id (cond
                        ;; Already a native UUID - use directly
                        (uuid? identifier)
                        identifier

                        ;; String — could be an XID (native ID) or username
                        ;; Try as ID first, then as name
                        (string? identifier)
                        (or
                          ;; Check if it's a known ID in cache
                          (when (get @cache identifier) identifier)
                          ;; Check name->id cache
                          (get-in @cache [::name->id identifier])
                          ;; Try DB lookup by ID
                          (when-let [user (try
                                            (get-user-details {(id/key) identifier})
                                            (catch Exception e
                                              (log/warnf e "[UserContext] Failed to load user by ID: %s" identifier)
                                              nil))]
                            (let [uid (id/extract user)]
                              (swap! cache (fn [c]
                                             (-> c
                                                 (assoc uid user)
                                                 (assoc-in [::name->id (:name user)] uid)
                                                 (assoc-in [::eid->id (:_eid user)] uid))))
                              uid))
                          ;; Try DB lookup by name
                          (when-let [user (try
                                            (get-user-details {:name identifier})
                                            (catch Exception e
                                              (log/warnf e "[UserContext] Failed to load user by name: %s" identifier)
                                              nil))]
                            (let [uid (id/extract user)]
                              (swap! cache (fn [c]
                                             (-> c
                                                 (assoc uid user)
                                                 (assoc-in [::name->id (:name user)] uid)
                                                 (assoc-in [::eid->id (:_eid user)] uid))))
                              uid)))

                        ;; Integer _eid - check eid->id mapping
                        (integer? identifier)
                        (or (get-in @cache [::eid->id identifier])
                            ;; Not in cache - need to load from DB
                            (when-let [user (try
                                              (get-user-details {:_eid identifier})
                                              (catch Exception e
                                                (log/warnf e "[UserContext] Failed to load user by _eid: %s" identifier)
                                                nil))]
                              ;; Cache the user data
                              (let [uid (id/extract user)]
                                (swap! cache (fn [c]
                                               (-> c
                                                   (assoc uid user)
                                                   (assoc-in [::name->id (:name user)] uid)
                                                   (assoc-in [::eid->id (:_eid user)] uid))))
                                uid)))

                        ;; Unknown identifier type
                        :else
                        (do
                          (log/warnf "[UserContext] Unknown identifier type: %s (%s)" identifier (type identifier))
                          nil))]

          ;; If we have an ID, check cache or load from DB
          (when user-id
            (or (get @cache user-id)
                ;; Not in cache - load by ID
                (when-let [user (try
                                  (get-user-details {(id/key) user-id})
                                  (catch Exception e
                                    (log/warnf e "[UserContext] Failed to load user by ID: %s" user-id)
                                    nil))]
                  ;; Cache the user data
                  (swap! cache (fn [c]
                                 (-> c
                                     (assoc user-id user)
                                     (assoc-in [::name->id (:name user)] user-id)
                                     (assoc-in [::eid->id (:_eid user)] user-id))))
                  user)))))

      (invalidate-user [_ identifier]
        (let [;; Resolve identifier to native ID
              user-id (cond
                        (uuid? identifier) identifier
                        (string? identifier)
                        (or (when (get @cache identifier) identifier)
                            (get-in @cache [::name->id identifier]))
                        (integer? identifier) (get-in @cache [::eid->id identifier])
                        :else nil)]

          (when user-id
            ;; Get user data to find all indexes
            (when-let [user (get @cache user-id)]
              ;; Remove from all indexes
              (swap! cache (fn [c]
                             (-> c
                                 (dissoc user-id)
                                 (update ::name->id dissoc (:name user))
                                 (update ::eid->id dissoc (:_eid user)))))
              (log/debugf "[UserContext] Invalidated user: %s" user-id))))
        nil)

      (clear-cache [_]
        (reset! cache {})
        (log/info "[UserContext] Cleared all cached user contexts")
        nil)

      (start! [this]
        ;; Subscribe to user entity changes
        (async/sub core/*delta-publisher* :iam/user delta-chan)

        ;; Start go-loop with alt! - exits cleanly when stop-chan closes
        (async/go-loop []
          (async/alt!
            delta-chan ([delta]
                        (when delta  ; nil means channel closed
                          (try
                            (when-let [entity-id (id/extract delta)]
                              (log/debugf "[UserContext] User changed, invalidating cache: %s" entity-id)
                              (invalidate-user this entity-id))
                            (catch Exception e
                              (log/error e "[UserContext] Error invalidating user from cache")))
                          (recur)))

            stop-chan ([_]  ; stop-chan closed - exit loop
                       (log/info "[UserContext] Cache invalidation stopped")
                       nil)))

        (log/info "[UserContext] Started automatic cache invalidation for user entity changes")
        nil)

      (stop! [_]
        ;; Close stop channel - go-loop exits via alt!
        (async/close! stop-chan)

        ;; Cleanup delta subscription
        (async/unsub core/*delta-publisher* :iam/user delta-chan)
        (async/close! delta-chan)
        nil))))

;; ============================================================================
;; Test Implementation: Atom Provider (No DB)
;; ============================================================================

(deftype AtomUserContextProvider [users]
  UserContextProvider

  (lookup-user [_ identifier]
    (let [user-id (cond
                    (uuid? identifier) identifier
                    (string? identifier)
                    (or
                      ;; Try as ID first (works for XID strings)
                      (when (get @users identifier) identifier)
                      ;; Try as username
                      (some (fn [[id user]]
                              (when (= (:name user) identifier)
                                id))
                            @users))
                    (integer? identifier)
                    (some (fn [[id user]]
                            (when (= (:_eid user) identifier)
                              id))
                          @users)
                    :else nil)]
      (when user-id
        (get @users user-id))))

  (invalidate-user [_ _]
    ;; No caching in atom provider - no-op
    nil)

  (clear-cache [_]
    ;; No caching in atom provider - no-op
    nil)

  (start! [_]
    ;; No lifecycle in atom provider - no-op
    nil)

  (stop! [_]
    ;; No lifecycle in atom provider - no-op
    nil))

;; ============================================================================
;; High-Level API
;; ============================================================================

(defn get-user-context
  "Retrieve user context using the current provider.

  Args:
    identifier - ID (UUID or XID), username (String), or _eid (Integer)

  Returns:
    User context map (see UserContextProvider protocol) or nil if not found.

  Throws:
    IllegalStateException if no provider is configured"
  [identifier]
  (when-not *user-context-provider*
    (throw (IllegalStateException. "No user context provider configured. Call set-user-context-provider! first.")))
  (lookup-user *user-context-provider* identifier))

(defmacro with-user-context
  "Bind user context to access control vars and execute body.

  Looks up user context using the current provider and binds:
  - access/*user* to the full user map
  - access/*roles* to the set of role IDs
  - access/*groups* to the set of group IDs

  Args:
    identifier - ID (UUID or XID), username (String), or _eid (Integer)
    body       - Expressions to evaluate with bound context

  Returns:
    Result of evaluating body

  Throws:
    IllegalStateException if no provider is configured
    IllegalArgumentException if user not found"
  [identifier & body]
  `(let [user-ctx# (get-user-context ~identifier)]
     (when-not user-ctx#
       (throw (IllegalArgumentException. (str "User not found: " ~identifier))))
     (binding [access/*user* user-ctx#
               access/*roles* (:roles user-ctx#)
               access/*groups* (:groups user-ctx#)]
       ~@body)))

;; ============================================================================
;; Initialization
;; ============================================================================

(defn start
  "Initialize the default user context provider.

  Creates a CachedUserContextProvider with automatic cache invalidation
  via delta channel subscription.

  Called automatically by synthigy.core/warmup and start."
  []
  (set-user-context-provider! (->CachedUserContextProvider)))
