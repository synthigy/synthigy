;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.apps.deploy
  "Runs a client's active package through the local portal and writes back its Status."
  (:require
   [clojure.string :as str]
   [synthigy.apps.model]
   [synthigy.apps.volumes :as volumes]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.env :as env]
   [synthigy.files :as files]
   [synthigy.files.http :as files-http]
   [synthigy.files.links :as links]
   [synthigy.files.storage :as storage]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.iam.gen :as gen]
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor])
  (:import
   [java.util.concurrent ExecutorService Executors]))

(def deploy-timeout-ms (* 10 60 1000))

(defonce ^ExecutorService executor (Executors/newSingleThreadExecutor))

(def package-selection
  {:xid nil :name nil :active nil :status nil :last_error nil :manifest nil :uploaded_on nil
   :app [{:selections {:xid nil :id nil :name nil :type nil :active nil :settings nil
                          :configuration [{:selections {:memory_limit_mb nil :cpu_limit_millicores nil :volume_limit_mb nil}}]
                          :variables [{:selections {:name nil :value nil :type nil}}]}}]
   :archive [{:selections {:xid nil}}]})

(defn package [xid]
  (access/with-principal nil
    (dataset/get-entity :apps/package {:xid xid} package-selection)))

(defn package-exists? [xid]
  (some? (access/with-principal nil (dataset/get-entity :apps/package {:xid xid} {:xid nil}))))

(defn write-package!
  "Write engine-owned fields of a package that still exists; never re-create a deleted one."
  [xid data]
  (when (package-exists? xid)
    (access/with-principal nil
      (dataset/stack-entity :apps/package (assoc data :xid xid)))))

(defn manifest-value [manifest k]
  (or (get manifest k) (get manifest (name k))))

(defn app-key
  "The portal's name for a client's app: its xid, which neither a rename nor a namesake can change."
  [client-xid]
  (str/lower-case client-xid))

(defn kebab
  "A client name as a hostname label: lowercase, every other run of characters one dash."
  [s]
  (-> (str s)
      (java.text.Normalizer/normalize java.text.Normalizer$Form/NFD)
      (str/replace #"\p{M}" "")
      str/lower-case
      (str/replace #"[^a-z0-9]+" "-")
      (str/replace #"^-+|-+$" "")))

(def label #"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?")

(def domain #"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?")

(defn reserved-labels []
  (into #{} (keep #(not-empty (str/lower-case (str/trim %))))
        (str/split (or (not-empty (env/setting :synthigy-apps-reserved-names)) "www") #",")))

(defn root-uri []
  (or (some-> env/iam-root-url not-empty java.net.URI.)
      (throw (ex-info "SYNTHIGY_IAM_ROOT_URL is not set, so the app has no address" {}))))

(defn box-host
  "The engine's own host name."
  []
  (let [h (some-> (root-uri) .getHost str/lower-case)]
    (when (or (str/blank? h) (re-matches #"[0-9.]+|\[.*\]" h))
      (throw (ex-info "SYNTHIGY_IAM_ROOT_URL has no host name, so apps have no address under it" {})))
    h))

(defn apps-domain
  "The domain an app's label rows live under: SYNTHIGY_APPS_DOMAIN, else the engine's own host."
  []
  (if-let [d (some-> (env/setting :synthigy-apps-domain) str/trim not-empty str/lower-case)]
    (if (re-matches domain d)
      d
      (throw (ex-info (str "SYNTHIGY_APPS_DOMAIN " (pr-str d) " is not a domain name: give a host name, no scheme or port") {})))
    (box-host)))

(defn under? [n parent]
  (or (= n parent) (str/ends-with? n (str "." parent))))

(defn reserved
  "Reserved labels, plus the engine's own when its host is a label under the apps domain."
  [apps engine]
  (let [own (when (and (not= apps engine) (under? engine apps))
              (subs engine 0 (- (count engine) (count apps) 1)))]
    (cond-> (reserved-labels)
      (and own (re-matches label own)) (conj own))))

(defn full-name
  "A hostname row as a full name: a label under the apps domain, or a custom domain outside it and the engine's host."
  [apps engine {:keys [name custom]}]
  (let [n (str name)]
    (cond
      (not custom)
      (cond
        (not (re-matches label n))
        (throw (ex-info (str "The hostname " (pr-str name) " is not a label of letters, digits and dashes") {}))
        ((reserved apps engine) n)
        (throw (ex-info (str "The hostname " n " is reserved on this engine") {}))
        :else (str n "." apps))

      (or (> (count n) 253) (not (re-matches domain n)))
      (throw (ex-info (str "The hostname " (pr-str name) " is not a domain name") {}))

      (under? n apps)
      (throw (ex-info (str "The custom hostname " n " is under " apps ", where an app's hostname is a label row") {}))

      (under? n engine)
      (throw (ex-info (str "The custom hostname " n " is under the engine's own name " engine) {}))

      :else n)))

(defn row-names
  "Full names of hostname rows, label rows first."
  [apps engine rows]
  (->> rows
       (sort-by (juxt #(boolean (:custom %)) #(str/lower-case (str (:name %)))))
       (map #(full-name apps engine %))
       distinct
       vec))

(defn hostname-rows [client-xid]
  (access/with-principal nil
    (:hostnames (dataset/get-entity :iam/app {:xid client-xid} {:hostnames [{:selections {:xid nil :name nil :custom nil}}]}))))

(defn generated-hostname!
  "Write an app's first hostname row, its client name as a label; a label another app holds is refused."
  [{:keys [xid name]}]
  (let [l (kebab name)]
    (when-not (re-matches label l)
      (throw (ex-info (str "The client name \"" name "\" gives no usable hostname; use letters or digits, or an admin sets one") {})))
    (when ((reserved (apps-domain) (box-host)) l)
      (throw (ex-info (str "The client name \"" name "\" gives the hostname " l ", which is reserved on this engine; an admin sets another") {})))
    (access/with-principal nil
      (let [held (first (dataset/search-entity :apps/hostname {:name {:_eq l}}
                                               {:xid nil :app [{:selections {:xid nil :name nil}}]}))
            owner (get-in held [:app :xid])]
        (when (and owner (not= owner xid))
          (throw (ex-info (str "The hostname " l " belongs to app \"" (get-in held [:app :name]) "\"; an admin gives this app another") {})))
        (dataset/stack-entity :apps/hostname {:xid (or (:xid held) (id/generate)) :name l :app {:xid xid}})
        {:name l}))))

(defn hosts
  "A service's full hostnames; its first deploy writes the generated row."
  [client]
  (row-names (apps-domain) (box-host) (or (seq (hostname-rows (:xid client))) [(generated-hostname! client)])))

(defn app-url
  "The URL an app is reached on: the root URL's scheme and port on the app's hostname."
  [host]
  (let [u (root-uri)]
    (str (.getScheme u) "://" host (when (pos? (.getPort u)) (str ":" (.getPort u))))))

(defn split-uri
  "[origin path] of a redirect entry, the path with its query and \"/\" when empty; nil when it is no absolute URI."
  [entry]
  (try
    (let [u (java.net.URI. entry)]
      (when (and (.getScheme u) (.getHost u))
        [(str (.getScheme u) "://" (.getHost u) (when (pos? (.getPort u)) (str ":" (.getPort u))))
         (str (if (str/blank? (.getRawPath u)) "/" (.getRawPath u))
              (when (.getRawQuery u) (str "?" (.getRawQuery u))))]))
    (catch Exception _ nil)))

(defn followed-redirections
  "An App's redirect entries once its hostnames move from `before` to `after`: every path it used on its own hosts, on each host in `after`, with each host's origin; entries on other origins stay."
  [entries before after]
  (let [ours (set (map app-url (concat before after)))
        split (map (juxt identity split-uri) entries)
        mine? (fn [[_ [origin]]] (contains? ours origin))
        paths (distinct (cons "/" (keep (fn [[_ [_ path] :as e]] (when (mine? e) path)) split)))]
    (vec (distinct (concat (keep (fn [[entry :as e]] (when-not (mine? e) entry)) split)
                           (for [h after p paths] (str (app-url h) p)))))))

(defn follow-hosts!
  "Move an App's redirect entries from the hostnames it had to the ones it has."
  [client-xid before after]
  (when (seq after)
    (access/with-principal nil
      (when-let [id (:id (dataset/get-entity :iam/app {:xid client-xid} {:id nil}))]
        (when (iam/update-client-settings! id update "redirections" #(followed-redirections (or % []) before after))
          (log/info {:id ::redirections-followed :data {:action :modified :subject :app :client client-xid :hosts (vec after)}}
                    "App redirect entries follow its hostnames"))))))

(defn hosts-error
  "Last Error for an app's certificates: each failed one, or nil."
  [hosts]
  (some->> (seq (filter #(= "failed" (some-> (:certificate %) name)) hosts))
           (map #(str "certificate for " (:name %) ": " (or (:error %) "failed")))
           (str/join "; ")))

(def default-memory-mb 256)

(def default-cpu-millicores 500)

(defn positive [what v]
  (when (some? v)
    (if (and (integer? v) (pos? v))
      v
      (throw (ex-info (str "The package's " what " must be a positive whole number, got " (pr-str v)) {})))))

(defn limits
  "What the manifest asks for, within the App Configuration, or the engine's defaults when it has none; asking for more is refused."
  [manifest {:keys [configuration]}]
  (let [ask (manifest-value manifest :resources)
        memory (positive "memoryMB" (manifest-value ask :memoryMB))
        cpu (positive "cpuMillicores" (manifest-value ask :cpuMillicores))
        {max-memory :memory_limit_mb max-cpu :cpu_limit_millicores} configuration
        max-memory (or max-memory default-memory-mb)
        max-cpu (or max-cpu default-cpu-millicores)]
    (when (and memory (> memory max-memory))
      (throw (ex-info (str "The package asks for " memory " MB of memory; this app may use " max-memory " MB") {})))
    (when (and cpu (> cpu max-cpu))
      (throw (ex-info (str "The package asks for " cpu " millicores of CPU; this app may use " max-cpu) {})))
    {:memoryMB (or memory (min default-memory-mb max-memory))
     :cpuMillicores (or cpu (min default-cpu-millicores max-cpu))}))

(def variable-name #"^[A-Za-z_][A-Za-z0-9_]*$")

(def max-variables 200)

(def max-variable-bytes (* 64 1024))

(def max-variables-bytes (* 256 1024))

(defn environment
  "An app's `environment`-type App Variables as env; a retired `file` one, a name set twice, a name the engine sets itself or an oversized value refuses the deploy."
  [variables]
  (when-let [retired (seq (keep #(when (= "file" (some-> (:type %) clojure.core/name)) (:name %)) variables))]
    (throw (ex-info (str "File variables are retired (" (str/join ", " retired) "); use environment or sdk") {})))
  (when-let [twice (seq (keep (fn [[n c]] (when (> c 1) n)) (frequencies (keep :name variables))))]
    (throw (ex-info (str "Variable " (str/join ", " twice) " is set more than once on this app") {})))
  (let [vars (into {} (for [{:keys [name value type]} variables
                            :when (and name (= "environment" (some-> type clojure.core/name)))]
                        [name (str value)]))
        size #(alength (.getBytes ^String % "UTF-8"))]
    (when (> (count vars) max-variables)
      (throw (ex-info (str "A package may carry " max-variables " variables, this one has " (count vars)) {})))
    (doseq [[k v] vars]
      (when (or (not (re-matches variable-name k)) (= "PORT" k) (str/starts-with? k "SYNTHIGY_"))
        (throw (ex-info (str "Variable \"" k "\": a name of letters, digits and _, not PORT or SYNTHIGY_*") {})))
      (when (> (size v) max-variable-bytes)
        (throw (ex-info (str "Variable " k " is larger than " (quot max-variable-bytes 1024) " KB") {}))))
    (when (> (reduce + (map (comp size val) vars)) max-variables-bytes)
      (throw (ex-info (str "The package's variables are larger than " (quot max-variables-bytes 1024) " KB together") {})))
    vars))

(defn health-path [manifest]
  (when-let [p (manifest-value manifest :healthCheckPath)]
    (if (and (string? p) (re-matches #"^/[^/#\\\s][^#\\\s]*$|^/$" p))
      p
      (throw (ex-info (str "healthCheckPath " (pr-str p) " must be a path starting with /") {})))))

(defn archive-source
  "A local path the portal can read: the blob itself on local storage, else a spooled copy to delete afterwards."
  [{:keys [archive manifest]}]
  (let [file-xid (or (:xid archive) (throw (ex-info "The package has no archive" {})))
        store files/*storage*
        f (files-http/fields)
        key (or (files-http/storage-key f file-xid)
                (throw (ex-info "The package archive has no stored version" {})))]
    (when-let [want (manifest-value manifest :archiveSha256)]
      (let [have (get (access/with-principal nil (files-http/file-row f file-xid)) (:hash f))]
        (when (not= want have)
          (throw (ex-info (str "The package archive changed after it was deployed (sha256 " have ", the package names " want ")") {})))))
    (if-let [f (when (satisfies? storage/FileBacked store) (storage/blob-file store key))]
      {:path (.getPath ^java.io.File f)}
      (let [{:keys [file]} (with-open [in (storage/open-blob store key)] (storage/spool! in))]
        {:path (.getPath ^java.io.File file) :spooled file}))))

(defn client-secret!
  "Grant client_credentials if missing and set a fresh secret; answers it in plain text."
  [{:keys [id type]}]
  (when (and id (= "confidential" (name (or type ""))))
    (access/with-principal nil
      (iam/update-client-settings! id update "allowed-grants" (fnil (comp vec distinct conj) []) "client_credentials")
      (let [secret (gen/client-secret)]
        (dataset/stack-entity :iam/app {:id id :secret secret})
        secret))))

(defn deploy-request
  "The portal request; a client with a running package keeps its secret, which the portal carries over, so a failed switch never breaks the running one."
  ([pkg running?] (deploy-request pkg running? nil))
  ([{:keys [xid manifest] client :app} running? secret]
  (let [endpoint (or (not-empty env/iam-root-url)
                     (throw (ex-info "SYNTHIGY_IAM_ROOT_URL is not set, so the app cannot be told where the engine is" {})))
        kind (or (manifest-value manifest :kind) "service")
        names (when (= "service" kind) (hosts client))
        env (cond-> (assoc (environment (:variables client)) "SYNTHIGY_ENDPOINT" endpoint)
              (:id client) (assoc "SYNTHIGY_CLIENT_ID" (:id client))
              (seq names) (assoc "SYNTHIGY_APP_URL" (app-url (first names))))
        vols (volumes/live (volumes/of-app (:xid client)))
        _ (volumes/assert-ready! vols)
        request (merge {:app (app-key (:xid client))
                        :package xid
                        :kind kind
                        :healthCheckPath (health-path manifest)}
                       (when names {:hosts names})
                       (when (seq vols) {:volumes (volumes/request vols)})
                       (limits manifest client))]
    (cond
      secret (assoc request :env (assoc env "SYNTHIGY_CLIENT_SECRET" secret))
      running? (assoc request :env env :inheritEnv ["SYNTHIGY_CLIENT_SECRET"])
      :else (let [secret (client-secret! client)]
              (assoc request :env (cond-> env secret (assoc "SYNTHIGY_CLIENT_SECRET" secret))))))))

(defn siblings [{xid :xid client :app}]
  (access/with-principal nil
    (->> (dataset/get-entity :iam/app {:xid (:xid client)}
                             {:packages [{:selections {:xid nil :active nil :status nil :uploaded_on nil}}]})
         :packages
         (remove #(= xid (:xid %))))))

(defn portal-apps []
  (:apps (supervisor/call-portal "app.status" {} 30000)))

(defn portal-app
  "What the portal runs for a client: its app.status entry, or nil."
  [client-xid]
  (some #(when (= (app-key client-xid) (:app %)) %) (portal-apps)))

(defn running? [p]
  (= "running" (some-> (:status p) name)))

(defn live?
  "Whether a portal app.status entry runs, counting a crashed App the portal is restarting."
  [entry]
  (contains? #{"running" "restarting"} (some-> (:status entry) name)))

(defn newer? [a b]
  (pos? (compare (:uploaded_on a) (:uploaded_on b))))

(defn took-over!
  "Mark the package running, its Last Error the failed certificates, and its client's other running or active packages stopped, except a newer one still waiting to deploy."
  ([pkg hosts] (took-over! pkg hosts nil))
  ([pkg hosts error]
  (write-package! (:xid pkg) {:status :running :last_error (or (not-empty error) (hosts-error hosts))})
  (follow-hosts! (get-in pkg [:app :xid]) [] (map :name hosts))
  (doseq [s (siblings pkg)
          :when (or (running? s) (:active s))
          :when (not (and (:active s) (nil? (:status s)) (newer? s pkg)))]
    (write-package! (:xid s) {:active false :status :stopped}))))

(def keep-packages 10)

(defn prune-packages!
  "Delete a client's packages beyond the newest `keep-packages`, never an active or running one, and their archives once no package uses them."
  [client-xid]
  (access/with-principal nil
    (let [pkgs (->> (dataset/get-entity :iam/app {:xid client-xid}
                                        {:packages [{:selections {:xid nil :active nil :status nil :uploaded_on nil
                                                                  :archive [{:selections {:xid nil}}]}}]})
                    :packages
                    (sort-by :uploaded_on #(compare %2 %1)))
          doomed (remove #(or (:active %) (running? %)) (drop keep-packages pkgs))]
      (when (seq doomed)
        (doseq [{:keys [xid]} doomed]
          (dataset/delete-entity :apps/package {:xid xid}))
        (let [used (into #{} (keep (comp :xid :archive))
                         (dataset/search-entity :apps/package nil {:archive [{:selections {:xid nil}}]}))]
          (doseq [f (distinct (keep (comp :xid :archive) doomed)) :when (not (used f))]
            (dataset/delete-entity :files/file {(id/key) f})
            (links/delete-of-file! f)))
        (log/info {:id ::pruned :data {:action :deleted :subject :app :client client-xid :packages (count doomed)}}
                  "Old app packages deleted")))))

(declare deploy-package!)

(defn deleted-while-deploying!
  "The portal switched to a package deleted meanwhile: switch back to the client's active package, or stop the app."
  [{client :app :as pkg}]
  (if-let [prev (some #(when (:active %) (:xid %)) (siblings pkg))]
    (deploy-package! prev)
    (do (supervisor/call-portal "app.stop" {:app (app-key (:xid client))} deploy-timeout-ms)
        (doseq [s (siblings pkg) :when (running? s)]
          (write-package! (:xid s) {:status :stopped}))))
  (log/info {:id ::deleted-while-deploying :data {:action :stopped :subject :app :package (:xid pkg) :client (:xid client)}}
            "App package deleted while it was deploying"))

(defn mounted
  "What the container of a portal app.status entry mounts, as #{[id path]}; path nil where the portal names ids only."
  [entry]
  (set (for [v (:volumes entry)] (if (map? v) [(:id v) (:path v)] [v nil]))))

(defn remount?
  "Whether a container mounting `now` must restart to mount `wanted`, both #{[id path]}."
  [wanted now]
  (if (some (comp nil? second) now)
    (not= (set (map first wanted)) (set (map first now)))
    (not= wanted now)))

(def volume-wait-ms (* 3 60 1000))

(defn await-volumes
  "The App's volumes once none is pending any more, or as they are when the wait runs out."
  [client-xid]
  (let [until (+ (System/currentTimeMillis) volume-wait-ms)]
    (loop []
      (let [vols (volumes/live (volumes/of-app client-xid))]
        (if (and (some #(= "pending" (volumes/state %)) vols) (< (System/currentTimeMillis) until))
          (do (Thread/sleep 500) (recur))
          vols)))))

(defn prepare-volumes!
  "Create the volume the manifest asks for when the App has none, tell the portal the App's volumes and wait until they are ready."
  [{:keys [manifest] client :app}]
  (let [limit (get-in client [:configuration :volume_limit_mb])
        ask (positive "volumeMB" (manifest-value manifest :volumeMB))]
    (when (and ask (empty? (volumes/live (volumes/of-app (:xid client)))))
      (when-let [e (volumes/limit-error limit [{:size_mb ask}])]
        (throw (ex-info e {})))
      (access/with-principal nil
        (dataset/stack-entity :apps/volume {:size_mb ask :app {:xid (:xid client)}})))
    (let [vols (volumes/live (volumes/of-app (:xid client)))]
      (when (seq vols)
        (when-let [e (volumes/limit-error limit vols)]
          (throw (ex-info e {})))
        (volumes/sync! (:xid client) (app-key (:xid client)))
        (await-volumes (:xid client))))))

(defn deploy-package!
  "Start the package through the portal; on success it becomes the client's only active package."
  [xid]
  (let [pkg (package xid)]
    (when (and pkg (:active pkg) (not= false (get-in pkg [:app :active]))
               (not-any? #(and (:active %) (newer? % pkg)) (siblings pkg)))
      (let [live (try (portal-app (get-in pkg [:app :xid])) (catch Throwable _ nil))]
        (if (and (live? live) (= xid (:package live)))
          (took-over! pkg (:hosts live) (when (= "restarting" (:status live)) (:error live)))
          (do
            (write-package! xid {:status :starting :last_error nil})
            (let [source (atom nil)]
              (try
                (let [_ (prepare-volumes! pkg)
                      request (deploy-request pkg (or (live? live) (boolean (some running? (siblings pkg)))))
                      archive (reset! source (archive-source pkg))
                      answer (supervisor/call-portal "app.deploy" (assoc request :archive (select-keys archive [:path])) deploy-timeout-ms)]
                  (if (package-exists? xid)
                    (do (took-over! pkg (:hosts answer))
                        (log/info {:id ::deployed :data {:action :deployed :subject :app :package xid :client (get-in pkg [:app :xid])}}
                                  "App package running")
                        (prune-packages! (get-in pkg [:app :xid])))
                    (deleted-while-deploying! pkg)))
                (catch Throwable t
                  (write-package! xid {:status :failed :active false :last_error (or (ex-message t) (str (class t)))})
                  (log/warn {:id ::deploy-failed :data {:action :deploying :subject :app :package xid :error (ex-message t)}}
                            "App package did not start"))
                (finally
                  (some-> @source :spooled storage/discard!))))))))))

(def rotation-ms (* 10 60 1000))

(defonce rotations (atom {}))

(defn mark-rotated!
  "Note that a person wrote an App's secret, for the reactor to act on once the write commits."
  [client-xid]
  (swap! rotations assoc client-xid (System/currentTimeMillis)))

(defn take-rotation!
  "True once for an App a person rotated recently."
  [client-xid]
  (let [[old _] (swap-vals! rotations dissoc client-xid)
        at (get old client-xid)]
    (boolean (and at (< (- (System/currentTimeMillis) at) rotation-ms)))))

(defn restart-with-new-secret!
  "Give the App's running package a secret the engine mints and restart it; a person's rotation leaves it a dead one."
  [client-xid]
  (when-let [{xid :xid} (first (filter running? (siblings {:app {:xid client-xid}})))]
    (let [pkg (package xid)
          source (atom nil)]
      (try
        (let [secret (or (client-secret! (:app pkg))
                         (throw (ex-info "The App is not a confidential client, so it has no secret to restart with" {})))
              request (deploy-request pkg true secret)
              archive (reset! source (archive-source pkg))
              answer (supervisor/call-portal "app.deploy"
                                             (assoc request :archive (select-keys archive [:path]) :restart true)
                                             deploy-timeout-ms)]
          (write-package! xid {:last_error (hosts-error (:hosts answer))})
          (log/info {:id ::secret-restarted :data {:action :restarted :subject :app :client client-xid :package xid}}
                    "App restarted with its new secret"))
        (catch Throwable t
          (write-package! xid {:last_error (str "The App still runs with its old secret, which no longer works: "
                                                (or (ex-message t) (str (class t))))})
          (log/warn {:id ::secret-restart-failed :data {:action :restarting :subject :app :client client-xid :package xid :error (ex-message t)}}
                    "App could not restart with its new secret"))
        (finally
          (some-> @source :spooled storage/discard!))))))

(declare submit!)

(defn on-secret-change [env]
  (when-let [xid (some-> env :delta :data :record-xid)]
    (submit! (fn [] (when (take-rotation! xid) (restart-with-new-secret! xid))))))

(defn stop-app!
  "Stop the client's app in the portal, when it runs one, and mark its running packages stopped."
  [client-xid]
  (let [running (filter running? (siblings {:app {:xid client-xid}}))]
    (when (seq running)
      (when (portal-app client-xid)
        (supervisor/call-portal "app.stop" {:app (app-key client-xid)} deploy-timeout-ms))
      (doseq [p running]
        (write-package! (:xid p) {:status :stopped}))
      (log/info {:id ::stopped :data {:action :stopped :subject :app :client client-xid}} "App stopped"))))

(defn stop-deleted!
  "Stop whatever the portal runs for a package or client that no longer exists."
  [pred]
  (doseq [{:keys [app]} (filter pred (portal-apps))]
    (supervisor/call-portal "app.stop" {:app app} deploy-timeout-ms)
    (log/info {:id ::stopped :data {:action :stopped :subject :app :app app}} "App of a deleted package stopped")))

(defn sync-hosts!
  "Give each running service the hostnames its rows name, every App giving names up before any takes one, and keep its package's Last Error on its certificates."
  []
  (when-let [apps (seq (try (portal-apps) (catch Throwable _ nil)))]
    (let [pkgs (into {} (comp (filter running?) (map (juxt :xid identity)))
                     (access/with-principal nil
                       (dataset/search-entity :apps/package {:active {:_eq true}}
                                              {:xid nil :status nil :last_error nil :manifest nil
                                               :app [{:selections {:xid nil :hostnames [{:selections {:name nil :custom nil}}]}}]})))
          live (for [{:keys [app package status hosts]} apps
                     :let [pkg (pkgs package)]
                     :when (and pkg (= "running" status) (= "service" (or (manifest-value (:manifest pkg) :kind) "service")))]
                 (let [wanted (try (row-names (apps-domain) (box-host) (get-in pkg [:app :hostnames]))
                                   (catch Throwable t t))]
                   {:app app :pkg pkg :hosts hosts :wanted wanted}))
          state (atom (into {} (map (fn [{:keys [app hosts]}] [app {:hosts hosts}])) live))
          send! (fn [app names]
                  (try (swap! state assoc app {:hosts (:hosts (supervisor/call-portal "app.hosts" {:app app :hosts names} deploy-timeout-ms))})
                       (catch Throwable t (swap! state assoc-in [app :error] (or (ex-message t) (str (class t)))))))]
      (doseq [{:keys [app hosts wanted]} live
              :when (vector? wanted)
              :let [keep (filterv (set wanted) (map :name hosts))]
              :when (< (count keep) (count hosts))]
        (send! app keep))
      (doseq [{:keys [app wanted]} live
              :when (and (vector? wanted) (not= (set wanted) (set (map :name (get-in @state [app :hosts])))))]
        (send! app wanted))
      (doseq [{:keys [pkg hosts wanted]} live
              :when (vector? wanted)]
        (follow-hosts! (get-in pkg [:app :xid]) (map :name hosts) wanted))
      (doseq [{:keys [app pkg wanted]} live
              :let [{:keys [hosts error]} (@state app)
                    error (cond (instance? Throwable wanted) (or (ex-message wanted) (str (class wanted)))
                                error error
                                :else (hosts-error hosts))]]
        (when (not= error (:last_error pkg))
          (write-package! (:xid pkg) {:last_error error}))))))

(defn delete-orphan-hostnames!
  "Delete hostname rows whose app is gone, so their names are free again."
  []
  (access/with-principal nil
    (doseq [{xid :xid client :app} (dataset/search-entity :apps/hostname nil {:xid nil :app [{:selections {:xid nil}}]})
            :when (nil? client)]
      (dataset/delete-entity :apps/hostname {:xid xid}))))

(defn submit! [f]
  (.submit executor ^Runnable
           (fn []
             (try (f)
                  (catch Throwable t
                    (log/error! {:id ::task-failed :msg "App deploy task failed" :data {:action :deploying :subject :app}} t))))))

(defonce sync-queued (atom false))

(defn queue-sync!
  "Run one hostname sync after whatever is queued; changes arriving meanwhile join it."
  []
  (when (compare-and-set! sync-queued false true)
    (submit! (fn [] (reset! sync-queued false) (sync-hosts!)))))

(defn on-package-change [env]
  (when-let [xid (some-> env :delta :data :record-xid)]
    (submit! (fn []
               (when-let [{:keys [active status] client :app} (package xid)]
                 (let [status (some-> status name)]
                   (cond
                     (and active (not (#{"starting" "running"} status))) (deploy-package! xid)
                     (and (not active) (= "running" status)) (stop-app! (:xid client)))))))))

(defn on-client-change [env]
  (when-let [xid (some-> env :delta :data :record-xid)]
    (submit! (fn []
               (let [{:keys [active packages]} (access/with-principal nil
                                                 (dataset/get-entity :iam/app {:xid xid}
                                                                     {:active nil :packages [{:selections {:xid nil :active nil :status nil}}]}))]
                 (if (false? active)
                   (stop-app! xid)
                   (some->> packages
                            (filter #(and (:active %) (not (#{"starting" "running"} (some-> (:status %) name)))))
                            first :xid deploy-package!)))))))


(defn app-event!
  "Put a portal's report of a crashed, recovered or vanished App, or of its certificates, on its package."
  [{xid :package :keys [app status error exitCode oomKilled restarts hosts] :as params}]
  (when-let [{:keys [active] :as pkg} (some-> xid str package)]
    (let [xid (:xid pkg)
          running (running? pkg)]
      (case (some-> status name)
        "restarting" (when running
                       (write-package! xid {:last_error (or (not-empty error) "The App crashed; the portal is restarting it")}))
        "running" (when running
                    (write-package! xid {:last_error nil})
                    (queue-sync!))
        "missing" (cond
                    active (deploy-package! xid)
                    running (write-package! xid {:status :stopped}))
        nil (when (and running (contains? params :hosts))
              (let [e (hosts-error hosts)]
                (when (not= e (:last_error pkg)) (write-package! xid {:last_error e}))))
        nil)
      (log/info {:id ::app-event :data {:action :modified :subject :app :app (get-in pkg [:app :xid]) :app-key app :package xid :status status
                                        :exit-code exitCode :oom-killed oomKilled :restarts restarts :error error}}
                "App event from the portal"))))

(defn restart-running!
  "Restart the App's running package with its secret kept, so it mounts the volumes it has now."
  [client-xid]
  (when-let [{xid :xid} (first (filter running? (siblings {:app {:xid client-xid}})))]
    (let [pkg (package xid)
          source (atom nil)]
      (try
        (let [request (deploy-request pkg true)
              archive (reset! source (archive-source pkg))
              answer (supervisor/call-portal "app.deploy"
                                             (assoc request :archive (select-keys archive [:path]) :restart true)
                                             deploy-timeout-ms)]
          (write-package! xid {:last_error (hosts-error (:hosts answer))})
          (log/info {:id ::volumes-restarted :data {:action :restarted :subject :app :client client-xid :package xid}}
                    "App restarted with its changed volumes"))
        (catch Throwable t
          (write-package! xid {:last_error (str "The App still runs with its old volumes: " (or (ex-message t) (str (class t))))})
          (log/warn {:id ::volumes-restart-failed :data {:action :restarting :subject :app :client client-xid :package xid :error (ex-message t)}}
                    "App could not restart with its changed volumes"))
        (finally
          (some-> @source :spooled storage/discard!))))))

(defn apply-volumes!
  "Tell the portal an App's volumes, then restart its running package when what it mounts changed."
  [client-xid]
  (volumes/sync! client-xid (app-key client-xid))
  (let [entry (try (portal-app client-xid) (catch Throwable _ nil))]
    (when (and (live? entry) (some running? (siblings {:app {:xid client-xid}})))
      (let [vols (await-volumes client-xid)
            wanted (set (map (juxt :id :path) (volumes/request vols)))]
        (when (remount? wanted (mounted entry))
          (restart-running! client-xid))))))

(defn on-volume-change [env]
  (when-let [xid (some-> env :delta :data :record-xid)]
    (submit! #(when-let [client (get-in (access/with-principal nil
                                          (dataset/get-entity :apps/volume {:xid xid} {:app [{:selections {:xid nil}}]}))
                                        [:app :xid])]
                (apply-volumes! client)))))

(defonce ^java.util.concurrent.ScheduledExecutorService sweeper
  (java.util.concurrent.Executors/newSingleThreadScheduledExecutor))

(def delete-retries-s [10 60 300 1800])

(defn delete-volume-data!
  "Delete a volume's data on the portal, retrying later while the portal refuses it (still mounted, root busy)."
  [xid retries]
  (try
    (volumes/delete-data! xid)
    (log/info {:id ::volume-data-deleted :data {:action :deleted :subject :app :volume xid}} "Volume data deleted")
    (catch Throwable t
      (log/warn {:id ::volume-delete-failed :data {:action :deleting :subject :app :volume xid :error (ex-message t)
                                                   :retry-in-s (first retries)}}
                "Volume data not deleted")
      (when-let [delay-s (first retries)]
        (.schedule sweeper ^Runnable (fn [] (submit! #(delete-volume-data! xid (rest retries))))
                   (long delay-s) java.util.concurrent.TimeUnit/SECONDS)))))

(defn volume-deleted!
  "A volume row a person deleted: restart the App whose container mounts it, as the portal reports, then delete its data."
  [xid]
  (when-let [entry (some #(when (contains? (set (map first (mounted %))) xid) %)
                         (try (portal-apps) (catch Throwable _ nil)))]
    (when-let [client (get-in (package (:package entry)) [:app :xid])]
      (restart-running! client)))
  (delete-volume-data! xid delete-retries-s))

(defonce sweep-task (atom nil))

(defn reconcile!
  "Make the portal run exactly the active packages of active clients, e.g. after the engine was down or deltas were dropped."
  []
  (submit! (fn []
             (when-let [apps (try (or (portal-apps) []) (catch Throwable _ nil))]
               (let [active (access/with-principal nil
                              (dataset/search-entity :apps/package {:active {:_eq true}}
                                                     {:xid nil :status nil :app [{:selections {:xid nil :active nil}}]}))
                     wanted (into {} (for [p active :when (not= false (get-in p [:app :active]))] [(:xid p) p]))
                     live (set (keep #(when (live? %) (:package %)) apps))]
                 (doseq [{:keys [app package]} apps :when (not (contains? wanted package))]
                   (supervisor/call-portal "app.stop" {:app app} deploy-timeout-ms)
                   (log/info {:id ::stopped :data {:action :stopped :subject :app :app app :package package}}
                             "App of an inactive package stopped"))
                 (doseq [[xid p] wanted :when (not (and (running? p) (live xid)))]
                   (deploy-package! xid))
                 (sync-hosts!)
                 (volumes/release-orphans!)
                 (volumes/sweep!))))))

(defn start! []
  (supervisor/register-method! "app.event"
                               (fn [params]
                                 (when (seq (:volumes params))
                                   (volumes/report! (:volumes params)))
                                 (submit! #(app-event! params))
                                 nil))
  (delta/subscribe! ::packages
                    {:entity-xids #{(str (id/entity :apps/package))}
                     :attribute-xids #{(str (id/data :apps.package/active))}
                     :ops #{:insert :update}}
                    on-package-change
                    (fn [_] (reconcile!)))
  (delta/subscribe! ::clients
                    {:entity-xids #{(str (id/entity :iam/app))}
                     :attribute-xids #{(str (id/data :apps.client/active))}
                     :ops #{:update}}
                    on-client-change)
  (delta/subscribe! ::secrets
                    {:entity-xids #{(str (id/entity :iam/app))}
                     :attribute-xids #{(str (id/data :apps.client/secret))}
                     :ops #{:update}}
                    on-secret-change)
  (delta/subscribe! ::package-deletes
                    {:entity-xids #{(str (id/entity :apps/package))} :ops #{:delete}}
                    (fn [env] (when-let [xid (some-> env :delta :data :record-xid)]
                                (submit! #(stop-deleted! (fn [{:keys [package]}] (= xid package)))))))
  (delta/subscribe! ::client-deletes
                    {:entity-xids #{(str (id/entity :iam/app))} :ops #{:delete}}
                    (fn [env] (when-let [xid (some-> env :delta :data :record-xid)]
                                (submit! #(do (stop-deleted! (fn [{:keys [app]}] (= (app-key xid) app)))
                                              (delete-orphan-hostnames!)
                                              (volumes/release-orphans!))))))
  (delta/subscribe! ::volumes
                    {:entity-xids #{(str (id/entity :apps/volume))}
                     :attribute-xids #{(str (id/data :apps.volume/size-mb)) (str (id/data :apps.volume/mount-path))}
                     :ops #{:insert :update}}
                    on-volume-change)
  (delta/subscribe! ::volume-deletes
                    {:entity-xids #{(str (id/entity :apps/volume))} :ops #{:delete}}
                    (fn [env] (when-let [xid (some-> env :delta :data :record-xid)]
                                (submit! #(volume-deleted! xid)))))
  (reset! sweep-task (.scheduleAtFixedRate sweeper ^Runnable (fn [] (submit! volumes/sweep!)) 1 1 java.util.concurrent.TimeUnit/HOURS))
  (delta/subscribe! ::hostnames
                    {:entity-xids #{(str (id/entity :apps/hostname))}
                     :relation-xids #{(str (id/relation :apps/app->hostnames))}}
                    (fn [_] (queue-sync!))
                    (fn [_] (queue-sync!)))
  (when (log/supervised?)
    (reconcile!)))

(defn stop! []
  (supervisor/unregister-method! "app.event")
  (some-> @sweep-task (.cancel false))
  (reset! sweep-task nil)
  (doseq [k [::packages ::clients ::secrets ::package-deletes ::client-deletes ::hostnames ::volumes ::volume-deletes]]
    (delta/unsubscribe! k)))
