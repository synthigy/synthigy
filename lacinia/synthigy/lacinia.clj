(ns synthigy.lacinia
  (:require
    [clojure.core.async :as async]
    [clojure.instant :refer [read-instant-date]]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia.introspection :as introspection]
    [com.walmartlabs.lacinia.parser.schema :refer [parse-schema]]
    [com.walmartlabs.lacinia.resolve :as r]
    [com.walmartlabs.lacinia.schema :as schema]
    [com.walmartlabs.lacinia.selection :as selection]
    [patcho.lifecycle :as lifecycle]
    synthigy.dataset
    [synthigy.transit
     :refer [<-transit ->transit]]))

(comment
  (-> compiled
      deref
      :Query
      :fields
      :__type)
  ;;
  (-> compiled
      deref
      :User :fields))

(defn deep-merge
  "Recursively merges maps."
  [& maps]
  (letfn [(m [& xs]
            (if (some #(and (map? %) (not (record? %))) xs)
              (apply merge-with m xs)
              (last xs)))]
    (reduce m maps)))

(defonce compiled (ref nil))
(defonce state (ref nil))

(def default-subscription-resolver
  ^r/ResolverResult (fn [_ _ v] (r/resolve-as v)))

(letfn [(parse-uuid [uuid]
          (cond
            (uuid? uuid) uuid
            (string? uuid) (java.util.UUID/fromString uuid)))
        (parse-date [date]
          (if (instance? java.util.Date date) date
              (try
                (read-instant-date date)
                (catch Exception _ nil))))]
  (def scalars
    {:null
     {:parse (constantly nil)
      :serialize (constantly nil)}
     :Timestamp
     {:description "Casts Date for internal usage"
      :parse parse-date
      :serialize identity}
     :UUID
     {:description "UUID"
      :parse parse-uuid
      :serialize identity}
     :JSON
     {:parse identity #_walk/stringify-keys
      :serialize identity}
     :Hash
     {:parse identity
      :serialize identity}
     :Transit
     {:parse <-transit
      :serialize ->transit}
     :Encrypted
     {:parse identity
      :serialize identity}}))

(defn bind-subscription-resolvers
  [schema]
  (letfn [(resolver [{:keys [directives]
                      :as v}]
            (if-let [{resolver-fn :fn}
                     (some
                       (fn [{:keys [directive-type directive-args]}]
                         (when (= directive-type :resolve)
                           directive-args))
                       directives)]
              (-> v
                  (assoc :stream (resolve (symbol resolver-fn))
                         :resolve default-subscription-resolver)
                  (update :directives
                          (fn [directives]
                            (vec
                              (remove
                                #(= :resolve (:directive-type %))
                                directives)))))
              v))
          (resolve-fields
            [mapping]
            (reduce-kv
              (fn [r k {:keys [directives]
                        :as v}]
                (if (not-empty directives)
                  (assoc r k (resolver v))
                  r))
              mapping
              mapping))]
    (cond->
      schema
      (some? (get-in schema [:objects :Subscription :fields]))
      (update-in [:objects :Subscription :fields] resolve-fields))))

(defn bind-resolvers
  [schema]
  (letfn [(resolver [{:keys [directives]
                      :as v}]
            (if-let [{resolver-fn :fn}
                     (some
                       (fn [{:keys [directive-type directive-args]}]
                         (when (= directive-type :resolve)
                           directive-args))
                       directives)]
              (-> v
                  (assoc :resolve (resolve (symbol resolver-fn)))
                  (update :directives
                          (fn [directives]
                            (vec
                              (remove
                                #(= :resolve (:directive-type %))
                                directives)))))
              v))
          ;;
          (resolve-fields
            [mapping]
            (reduce-kv
              (fn [r k {:keys [directives]
                        :as v}]
                (if (not-empty directives)
                  (assoc r k (resolver v))
                  r))
              mapping
              mapping))
          ;;
          (bind-resolvers [schema k]
            (update schema k
                    (fn [objects]
                      (reduce-kv
                        (fn [os k v]
                          (assoc os k (update v :fields resolve-fields)))
                        objects
                        objects))))]
    (->
      schema
      bind-subscription-resolvers
      (bind-resolvers :objects)
      (bind-resolvers :input-objects))))

(defn default-field-resolver
  "The default for the :default-field-resolver option, this uses the field name as the key into
  the resolved value."
  [{_alias :alias}]
  ^{:tag r/ResolverResult}
  (fn default-resolver [ctx _ v]
    (let [_alias (selection/alias-name (-> ctx :com.walmartlabs.lacinia/selection))]
      (r/resolve-as (get v _alias)))))

; (defn protect-schema
;   [schema]
;   (-> 
;     schema
;     (deep-map-merge (introspection/introspection-schema))
;     (update-in [:queries :__type :resolve]
;               (fn [resolver]
;                 (wrap-protect {:scopes ["dataset:graphiql"]} resolver)))
;     (update-in [:queries :__schema :resolve]
;                (fn [resolver]
;                  (wrap-protect {:scopes ["dataset:graphiql"]} resolver)))))

(defonce __schema (introspection/introspection-schema))

(defn ^:private recompile []
  (let [{:keys [shards directives]} @state]
    (when (not-empty shards)
      (log/infof
        "Recompiling shards: [%s]"
        (str/join ", " (keys shards)))
      (when-some [schema (reduce
                           deep-merge
                           (map
                             (fn [s] (if (fn? s) (s) s))
                             (vals shards)))]
        ; (do
        ;     (def schema schema)
        ;     (def directives directives)
        ;     (->
        ;      schema
        ;      bind-resolvers
        ;      (deep-merge __schema)))
        (schema/compile
          ; schema
          (->
            schema
            bind-resolvers
            (deep-merge __schema))
          {:enable-introspection? false
           :default-field-resolver default-field-resolver
           :apply-field-directives
           (fn [field resolver-fn]
             (log/infof
               "Applying field directive to field %s"
               (pr-str (selection/field-name field)))
             (let [field-directives (selection/directives field)
                   matching-directives (filter
                                         #(contains? directives %)
                                         (keys field-directives))]
               ;; When there are matching directives
               (if (not-empty matching-directives)
                 ;; Than for each of directives
                 (reduce
                   (fn [v k]
                     ;; Try to get transform
                     (if-let [xf ((get directives k) (get field-directives k) v)]
                       ;; And if successfull than wrap resolver with that transformation
                       ;; Transformation should be function of 4 keys
                       ;; context args original-value resolved-value
                       xf
                       v))
                   resolver-fn
                   (sort-by :metric matching-directives))
                 resolver-fn)))})))))

(comment
  (dosync (ref-set compiled (recompile))))

(defn remove-directive [key]
  (dosync
    (alter state update :directives dissoc key)
    (ref-set compiled (recompile))))

(defn add-directive [key shard]
  (dosync
    (alter state assoc-in [:directives key] shard)
    (ref-set compiled (recompile))))

(defn directives [] (keys (:directives @state)))
(defn directive [key] (get-in @state [:directives key]))

(defn remove-shard [key]
  (dosync
    (alter state update :shards clojure.core/dissoc key)
    (ref-set compiled (recompile))))

(defn add-shard
  "Add shard to global GraphQL schema. Specify id (key) of shard
  and shard schema in form of a string."
  [key shard]
  (log/infof "Adding Lacinia shard %s" key)
  (let [shard (cond
                (string? shard) (parse-schema shard)
                (fn? shard) (shard)
                :else shard)]
    ; (def shard shard)
    (dosync
      (alter state assoc-in [:shards key] shard)
      (ref-set compiled (recompile)))))

(defn shards [] (keys (:shards @state)))

(defn shard [key] (get-in @state [:directives key]))

;;; ============================================================================
;;; Model Event Listener
;;; ============================================================================

(defonce ^:private model-listener-chan (atom nil))

(defn- reload!
  ([] (reload! (synthigy.dataset/deployed-model)))
  ([model]
   (try
     (comment
       (def generate-schema (requiring-resolve 'synthigy.dataset.lacinia/generate-lacinia-schema))
       (def model (synthigy.dataset/deployed-model)))
     ;; Use requiring-resolve to avoid circular dependency
     ;; Generate schema directly from model (no db access)
     (let [generate-schema (requiring-resolve 'synthigy.dataset.lacinia/generate-lacinia-schema)]
       (add-shard ::datasets (fn [] (generate-schema model))))
     (catch Throwable e
       (log/error e "Failed to regenerate GraphQL schema on model deployment")))))

(defn start-model-listener!
  "Starts listening to :model/deployed events and regenerates GraphQL schema.

  This establishes the event-driven architecture where dataset publishes
  model changes and lacinia automatically regenerates the schema.

  Args:
    publisher - core.async publisher from synthigy.dataset/publisher

  Returns: nil

  The listener will automatically regenerate the ::datasets shard whenever
  a :model/deployed event is received. Schema is generated directly from
  the model in the event (no database access needed)."
  []
  (when-let [old-chan @model-listener-chan]
    (log/info "Stopping existing model listener")
    (async/close! old-chan))

  (let [sub (async/chan)
        publisher synthigy.dataset/publisher]
    (reset! model-listener-chan sub)
    (async/sub publisher :model/deployed sub)
    (log/info "Started Lacinia model listener for :model/deployed events")

    (async/go-loop [{:keys [model]
                     :as event} (async/<! sub)]
      (when event
        (log/info "Received :model/deployed event, regenerating GraphQL schema")
        (reload! model)
        (recur (async/<! sub)))))
  nil)

(defn stop-model-listener!
  "Stops the model deployment event listener."
  []
  (when-let [chan @model-listener-chan]
    (log/info "Stopping Lacinia model listener")
    (async/close! chan)
    (reset! model-listener-chan nil))
  nil)

;;; ============================================================================
;;; Initialization
;;; ============================================================================

(defn start
  "Initializes the Lacinia GraphQL system.

  This sets up all GraphQL infrastructure:
  1. Registers dataset hooks directive
  2. Loads static GraphQL schema shards (directives and base schema)
  3. Starts model change listener for automatic schema regeneration

  This should be called during application startup after dataset initialization."
  []
  (log/info "Initializing Lacinia GraphQL system...")
  (try
    (reload!)
    ;; Register the :hook directive with wrap-hooks function from dataset
    (let [wrap-hooks (requiring-resolve 'synthigy.dataset/wrap-hooks)]
      (add-directive :hook wrap-hooks))

    ;; Load static GraphQL schema shards from resources
    (add-shard ::dataset-directives (slurp (io/resource "dataset_directives.graphql")))
    (add-shard ::dataset-extensions (slurp (io/resource "datasets.graphql")))

    ;; Start event listener for automatic schema regeneration on model changes
    (start-model-listener!)

    (log/info "Lacinia GraphQL system initialized")
    (catch Throwable e
      (log/error e "Failed to initialize Lacinia GraphQL system"))))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/graphql
  {:depends-on [:synthigy/dataset]
   :start (fn []
            ;; Runtime: Initialize GraphQL system, start model listener
            (log/info "[GRAPHQL] Starting GraphQL system...")
            (start)
            (log/info "[GRAPHQL] GraphQL system started"))
   :stop (fn []
           ;; Runtime: Stop model listener
           (log/info "[GRAPHQL] Stopping GraphQL system...")
           (stop-model-listener!)
           (log/info "[GRAPHQL] GraphQL system stopped"))})
