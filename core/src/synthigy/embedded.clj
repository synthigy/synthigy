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

(ns synthigy.embedded
  "The Synthigy data API in-process — the SDK surface, dispatched straight at
   the engine."
  (:refer-clojure :exclude [sync get])
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.runtime :as runtime]
   [synthigy.embedded.selection :as selection]
   [synthigy.embedded.subscription :as subscription]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.engine :as data]
   [synthigy.engine.history :as history]))

(def ^:dynamic *key-format*
  "Process-wide default response key format; a per-call :key-format wins."
  "kebab")

(defn resolve-principal
  "The principal a call runs as."
  [{:keys [acting-as principal]}]
  (if acting-as
    (let [user-ctx (iam.context/get-user-context acting-as)]
      (cond
        (nil? user-ctx)
        (throw (ex-info "User not found"
                        {:code "USER_NOT_FOUND" :acting-as acting-as}))

        (false? (:active user-ctx))
        (throw (ex-info "User is inactive"
                        {:code "USER_INACTIVE" :acting-as acting-as}))

        :else user-ctx))
    (clojure.core/or principal access/*principal*)))

(defn key-fn-for
  [opts]
  (let [kf (if (contains? opts :key-format) (:key-format opts) *key-format*)]
    (when kf (clojure.core/get dk/format->key-fn kf))))

(defn run
  "Run ONE operation and return its shaped data; exceptions propagate as-is."
  [op-map opts]
  (let [principal (resolve-principal opts)
        key-fn    (key-fn-for opts)
        started   (System/currentTimeMillis)]
    (access/with-principal principal
      (let [op-map (data/ensure-tree-on-relation op-map)
            ctx    (data/operation-context {:principal principal} op-map)
            result (try
                     (let [r (:result (data/execute-operation ctx))]
                       (data/maybe-audit-op! ctx started {:ok true :data r})
                       r)
                     (catch Throwable e
                       (data/maybe-audit-op! ctx started
                                             {:ok false
                                              :error {:message (ex-message e)
                                                      :code (:code (ex-data e) "INTERNAL_ERROR")}
                                              :synthigy.server.data/throwable e})
                       (throw e)))]
        (data/shape-data key-fn (select-keys ctx [:entity :entity-id :selections]) result)))))

(defn search
  "Search for entities matching args; returns a vector, never nil."
  [entity args selection & {:as opts}]
  (run {:op "search" :entity (name entity) :args args
        :selections (selection/normalize selection)}
       opts))

(defn get
  "Get a single entity by unique constraint."
  [entity args selection & {:as opts}]
  (run {:op "get" :entity (name entity) :args args
        :selections (selection/normalize selection)}
       opts))

(defn sync
  "Sync (upsert) entity data — returns {:count n}. Pass `:returning true`
   for the written records; see docs/plans/PLAN-SYNC-RETURNING-FLAG.md."
  [entity data & {:keys [returning] :as opts}]
  (run {:op "sync" :entity (name entity) :data data :returning (boolean returning)} opts))

(defn stack
  "Stack data on top of current state — returns {:count n}. Same
   `:returning` contract as sync."
  [entity data & {:keys [returning] :as opts}]
  (run {:op "stack" :entity (name entity) :data data :returning (boolean returning)} opts))

(defn delete
  "Delete entity records."
  [entity data & {:as opts}]
  (run {:op "delete" :entity (name entity) :data data} opts))

(defn slice
  "Slice relations from entity; the selection names link-sets to cut."
  [entity args selection & {:as opts}]
  (run {:op "slice" :entity (name entity) :args args
        :selections (selection/normalize selection)}
       opts))

(defn purge
  "Find and delete matching records, returning the deleted data."
  [entity args selection & {:as opts}]
  (run {:op "purge" :entity (name entity) :args args
        :selections (selection/normalize selection)}
       opts))

(defn sql-template
  "Execute an ERD-aware SQL template."
  [template params & {:keys [cached] :or {cached true} :as opts}]
  (run {:op "sql-template" :template template :params params :cached cached}
       opts))

(defn search-tree
  "Search matching entities + walk `on` relation UP to their ancestors."
  [entity on args selection & {:as opts}]
  (run {:op "search-tree" :entity (name entity) :on (name on) :args args
        :selections (selection/normalize selection)}
       opts))

(defn get-tree
  "From `root` entity id, return root + descendants reachable via `on`."
  [entity root on selection & {:as opts}]
  (run {:op "get-tree" :entity (name entity) :root root :on (name on)
        :selections (selection/normalize selection)}
       opts))

(defn xsql-document
  "Ensure an XSQL operation document; bare rooted bodies get a synthetic `@<op>
   _q` header."
  [source op]
  (if (str/starts-with? (str/triml source) "@")
    source
    (str "@" (name op) " _q\n" source)))

(defn query
  "Run an XSQL document with optional ?name:type[] params."
  [xsql params & {:keys [op] :or {op "search"} :as opts}]
  (run (cond-> {:op "xsql" :xsql (xsql-document xsql op)}
         params (assoc :params params))
       opts))

(defn schema
  "The IAM-projected runtime schema, optionally narrowed to a seq of entity
   names."
  ([] (schema nil))
  ([entities & {:as opts}]
   (run (cond-> {:op "schema"}
          (seq entities) (assoc :entities (mapv name entities)))
        (assoc opts :key-format nil))))

(defn lint
  "Diagnostics for an XSQL `source` against the IAM-projected schema."
  [source & {:keys [entity op] :as opts}]
  (access/with-principal (resolve-principal opts)
    (data/lint-source (cond-> {:source source}
                        entity (assoc :entity (name entity))
                        op (assoc :op (name op))))))

(defn deployed-model
  "The raw deployed ERD model as a Clojure value; requires the dataset:load
   scope."
  [& {:as opts}]
  (access/with-principal (resolve-principal opts)
    (daccess/protect-model (dataset/deployed-model))))

(defn runtime-model
  "The RUNTIME model — deployed plus identity, audit attrs, and
   reference-as-relations."
  [& {:as opts}]
  (access/with-principal (resolve-principal opts)
    (-> (dataset/deployed-model) daccess/protect-model runtime/build)))

(defn describe
  "Compile an XSQL program `source` to codegen IR — `{:operations [...]}`."
  [source & {:as opts}]
  (run {:op "describe" :source source} (assoc opts :key-format nil)))

(defn history
  [op hopts opts]
  (:result (history/execute {:op op :opts hopts} (resolve-principal opts))))

(defn between-or-now
  [between]
  (clojure.core/or between [nil (str (java.time.Instant/now))]))

(defn history-get-at
  "State of `record-xid` as of timestamp `at`."
  [record-xid at & {:keys [tenant include-deleted?] :as opts}]
  (history "get-at"
           (cond-> {:record-xid record-xid :at at}
             tenant (assoc :tenant tenant)
             (some? include-deleted?) (assoc :include-deleted? include-deleted?))
           opts))

(defn history-events
  "Events for `record-xid` (nil = any record) over a time range."
  [record-xid & {:keys [between tenant limit track] :as opts}]
  (history "events"
           (cond-> {:between (between-or-now between)}
             record-xid (assoc :record-xid record-xid)
             tenant (assoc :tenant tenant)
             limit (assoc :limit limit)
             track (assoc :track track))
           opts))

(defn history-diff
  "How a record's state differs between two timestamps."
  [record-xid from-ts to-ts & {:keys [tenant] :as opts}]
  (history "diff"
           (cond-> {:record-xid record-xid :from-ts from-ts :to-ts to-ts}
             tenant (assoc :tenant tenant))
           opts))

(defn history-timeline
  "Events grouped by `:group-by` (\":request\" / \":actor\" / \":scope\")."
  [& {:keys [between group-by tenant limit] :as opts}]
  (history "timeline"
           (cond-> {:between (between-or-now between)}
             group-by (assoc :group-by group-by)
             tenant (assoc :tenant tenant)
             limit (assoc :limit limit))
           opts))

(defn history-since
  "Events strictly after the `:cursor` timestamp, oldest-first."
  [& {:keys [cursor tenant limit track] :as opts}]
  (history "since"
           (cond-> {}
             cursor (assoc :cursor cursor)
             tenant (assoc :tenant tenant)
             limit (assoc :limit limit)
             track (assoc :track track))
           opts))

;; COPIED CONTRACT — tree composition ported from synthigy.client.core; keep in
;; step.

(defn record-id
  [record]
  (some #(clojure.core/get record %) [:xid :euuid :_eid "xid" "euuid" "_eid"]))

(defn parent-id
  [record on]
  (let [k (if (keyword? on) on (keyword on))
        snake (keyword (str/replace (name k) \- \_))
        kebab (keyword (str/replace (name k) \_ \-))
        v (clojure.core/or (clojure.core/get record k)
                           (and (not= k snake) (clojure.core/get record snake))
                           (and (not= k kebab) (clojure.core/get record kebab)))]
    (cond
      (nil? v) nil
      (map? v) (record-id v)
      :else v)))

(defn tree-build-indexes
  [records on]
  (reduce
   (fn [[by-id kids] r]
     (let [id (record-id r)
           pid (parent-id r on)]
       (cond
         (nil? id) [by-id kids]
         (and pid (not= id pid))
         [(assoc by-id id r)
          (update kids pid (fnil conj []) id)]
         :else
         [(assoc by-id id r) kids])))
   [{} {}]
   records))

(defn build-subtree
  "Recursively build the subtree rooted at `id`; cycles bail with nil."
  [by-id kids children-key id visited]
  (when-not (contains? visited id)
    (let [visited' (conj visited id)
          record (clojure.core/get by-id id)]
      (assoc record
             children-key
             (into []
                   (keep #(build-subtree by-id kids children-key % visited'))
                   (clojure.core/get kids id []))))))

(defn compose-tree
  "Compose flat records (each carrying its parent FK) into one nested tree
   rooted at `root-id`."
  [records {:keys [on root-id children-key]
            :or   {children-key :children}}]
  (when (and (seq records) on)
    (let [[by-id kids] (tree-build-indexes records on)
          root (clojure.core/or root-id (record-id (first records)))]
      (when (contains? by-id root)
        (build-subtree by-id kids children-key root #{})))))

(defn compose-forest
  "Compose flat records into a forest — one tree per record whose parent isn't
   in the set."
  [records {:keys [on children-key]
            :or   {children-key :children}}]
  (when (and (seq records) on)
    (let [[by-id kids] (tree-build-indexes records on)
          roots (keep (fn [r]
                        (let [id (record-id r)
                              pid (parent-id r on)]
                          (when (clojure.core/or (nil? pid)
                                                 (not (contains? by-id pid))
                                                 (= id pid))
                            id)))
                      records)]
      (mapv #(build-subtree by-id kids children-key % #{}) roots))))

(defn search-tree-composed
  [entity on args selection & {:keys [children-key]
                               :or   {children-key :children}
                               :as opts}]
  (let [records (run {:op "search-tree" :entity (name entity) :on (name on) :args args
                      :selections (selection/normalize selection)}
                     opts)]
    (compose-forest records {:on on :children-key children-key})))

(defn get-tree-composed
  [entity root on selection & {:keys [children-key]
                               :or   {children-key :children}
                               :as opts}]
  (let [records (run {:op "get-tree" :entity (name entity) :root root :on (name on)
                      :selections (selection/normalize selection)}
                     opts)]
    (compose-tree records {:on on :children-key children-key :root-id root})))

(defn watch
  "Subscribe to change events and stream them to `on-event`; returns a mutable
   handle for close-watch!."
  [interest on-event & {:keys [coalesce-ms] :as opts}]
  (subscription/assert-ready!)
  (let [principal (resolve-principal opts)
        key       (keyword "synthigy.embedded" (str "watch-" (gensym)))
        ;; roles resolved ONCE and closed over — the delivery loop runs outside
        ;; any principal scope
        roles     (access/project-role-xids principal)
        state     (atom interest)
        deliver!  (fn [envelope]
                    (try
                      (run! on-event
                            (subscription/envelope->events
                             envelope (set (map str (:records @state))) roles))
                      (catch Throwable e
                        (binding [*out* *err*]
                          (println "synthigy.embedded watch handler threw:"
                                   (ex-message e))))))
        handler   (if coalesce-ms
                    (let [gen (atom 0)]
                      (fn [envelope]
                        (let [g (swap! gen inc)]
                          (a/go
                            (a/<! (a/timeout coalesce-ms))
                            (when (= g @gen)
                              (a/thread (deliver! envelope)))))))
                    deliver!)
        retune!   (fn [i]
                    (delta/retune!
                     key
                     (access/with-principal principal (subscription/interest i)))
                    i)]
    (delta/subscribe! key
                      (access/with-principal principal
                        (subscription/interest interest))
                      handler)
    {:key key
     :interest     (fn [] @state)
     :set-interest (fn [i] (retune! (reset! state i)))
     :add          (fn [xids] (retune! (swap! state update :records
                                             (fnil into []) xids)))
     :remove       (fn [xids] (let [s (set (map str xids))]
                                (retune! (swap! state update :records
                                                #(vec (remove (comp s str) %))))))
     :close        (fn [] (delta/unsubscribe! key))}))

(declare live-value)

(defn debounced
  "Call `f` after `ms` of quiet; a burst of N signals costs one `f`."
  [gen ms f]
  (let [g (swap! gen inc)]
    (a/go
      (a/<! (a/timeout ms))
      (when (= g @gen)
        (a/thread (f))))))

(defn watch-query
  "A live result set as an atom, refetched on change — refetching (never event
   payloads) is what keeps rows RLS-correct."
  [entity args selection & {:as opts}]
  (let [run-opts (select-keys opts [:acting-as :principal :key-format])
        run      #(apply search entity args selection (mapcat identity run-opts))]
    (live-value run (name entity) opts)))

(defn live-value
  "Shared engine of `watch-query` and `watch-xsql`: snapshot atom + interest +
   coalesced notify-then-refetch."
  [run entity-name {:keys [relations records entity-track debounce-ms entities]
                    :or {entity-track true debounce-ms 80}
                    :as opts}]
  (subscription/assert-ready!)
  (when (and (not entity-track) (empty? records) (empty? relations)
             (empty? entities))
    (throw (ex-info (str ":entity-track false needs :records, :relations or "
                         ":entities — otherwise the interest is empty, which "
                         "delta reads as the firehose, not as silence.")
                    {:code "UNREACHABLE_INTEREST"})))
  (let [base-entities (clojure.core/or entities
                                       (when entity-track [entity-name]))
        interest (cond-> {}
                   (seq base-entities) (assoc :entities (vec base-entities))
                   (seq relations)     (assoc :relations relations)
                   (seq records)       (assoc :records (vec records)))
        value    (atom (run))
        gen      (atom 0)
        w        (apply watch
                        interest
                        (fn [_event]
                          (debounced gen debounce-ms
                                     #(try (reset! value (run))
                                           (catch Throwable e
                                             (binding [*out* *err*]
                                               (println "synthigy.embedded live refetch failed:"
                                                        (ex-message e)))))))
                        (mapcat identity (select-keys opts [:acting-as :principal])))]
    (alter-meta! value assoc ::close (:close w) ::watch-key (:key w))
    value))

(defn run-xsql
  "Run a compiled XSQL op map `{:op :source :entity}` with a params map — the
   entry point generated code calls."
  ([op-map params] (run-xsql op-map params nil))
  ([{:keys [op source entity]} params opts]
   (if (= op "sql-template")
     (apply sql-template source params (mapcat identity opts))
     (apply query source params :op op :entity entity (mapcat identity opts)))))

(defn watch-xsql
  "`watch-query` for a compiled XSQL op map — what `@watch` in a `.xsql`
   compiles to."
  ([op-map params] (watch-xsql op-map params nil))
  ([op-map params opts]
   (let [run-opts (select-keys opts [:acting-as :principal :key-format])
         run      #(run-xsql op-map params run-opts)]
     (live-value run (:entity op-map) opts))))

(defn close-watch!
  "Stop a handle from `watch` or a live atom from `watch-query`."
  [h]
  (if (map? h)
    ((:close h))
    (when-let [c (::close (meta h))] (c)))
  nil)

(comment
  (search :iam/user nil {:name nil}))
