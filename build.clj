;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns build
  "Uberjar for a chosen backend combo. Combo via SYNTHIGY_COMBO env,
   e.g. SYNTHIGY_COMBO=engine-sqlite:audit-sqlite:log-sqlite:server-httpkit:server-console. Default build bundles
   sources (no AOT). SYNTHIGY_AOT=true compiles every local namespace and
   strips local .clj/.cljc/.cljs from the jar — bytecode-only service
   artifact for GitHub releases; source publishing is Clojars, not this."
  (:require [clojure.edn]
            [clojure.tools.build.api :as b]
            [clojure.tools.namespace.find :as find]
            [clojure.string :as str]
            [deps-deploy.deps-deploy :as dd]))

;; test-only namespaces whose deps aren't on the shipped combos' classpath
(def no-aot '#{synthigy.oauth.client})

;; resources/public/js is gitignored frontend build output (data console,
;; modeler — a separate project) that nothing in core serves; it must never
;; reach a jar, least of all its .js.map files, which embed .cljs source.
;; Excluded at b/uber, which matches full paths — copy-dir :ignores are
;; matched against the file NAME, so a directory pattern silently misses.
(def never-ship "public/js/.*")

;; under synthigy/, not the jar root: b/uber append-merges every dependency's root/META-INF LICENSE and NOTICE into one file
(def license-files ["LICENSE" "NOTICE.md" "COMMERCIAL.md"])

(def combo (or (System/getenv "SYNTHIGY_COMBO") "engine-postgres:audit-postgres:log-sqlite:server-httpkit:server-console"))
(def aot? (= "true" (System/getenv "SYNTHIGY_AOT")))
(def aliases (mapv keyword (str/split combo #":")))
(def class-dir "target/classes")
(def uber-file (format "target/synthigy-%s.jar" (str/replace combo ":" "-")))

(defn uber [_]
  (let [basis (b/create-basis {:aliases aliases})
        dirs  (filter #(.isDirectory (java.io.File. ^String %))
                      (keys (:classpath basis)))]
    ;; delete only the class dir — a matrix build accumulates one jar per
    ;; combo in target/, wiping it would keep just the last
    (b/delete {:path class-dir})
    (doseq [d dirs]
      (b/copy-dir (cond-> {:src-dirs [d] :target-dir class-dir}
                    aot? (assoc :ignores [#".*\.clj[cs]?$"]))))
    (doseq [f license-files]
      (b/copy-file {:src f :target (str class-dir "/synthigy/" f)}))
    (when-let [v (System/getenv "SYNTHIGY_VERSION")]
      (b/write-file {:path (str class-dir "/synthigy/VERSION") :string v}))
    (when aot?
      (let [nses (remove no-aot (mapcat #(find/find-namespaces-in-dir (java.io.File. ^String %)) dirs))]
        (b/compile-clj {:basis basis :class-dir class-dir :ns-compile (vec nses)})))
    ;; :local/root lib sources enter via b/uber, not copy-dir — any local
    ;; lib outside the synthigy/patcho packages needs its own exclude here
    (b/uber {:class-dir class-dir :uber-file uber-file :basis basis
             :exclude (cond-> [never-ship]
                        aot? (conj "(?:synthigy|patcho)/.*\\.clj[cs]?"))})
    (println "Built" uber-file (if aot? "(AOT)" "(source)"))))

;; --- modular build: core + one jar per module ------------------------------
;; Composed on the classpath instead of fused into a combo uberjar. See
;; docs/plans/PLAN-MODULAR-JARS.md and PLAN-DISTRIBUTION.md. Always AOT: the
;; point is shipping the edge tree as bytecode.

;; core (base :paths — the engine, see deps.edn) has NOTHING baked in beyond
;; itself. Everything else composes on top, one strict layer at a time:
;;
;;   core -> server-oauth -> server-core -> {server-httpkit, server-console}
;;
;; Each module is compiled with the PREVIOUS layer's class dir staged in, and
;; ships only what it added (`purge-foreign!`). server-oauth depends on core
;; alone — deliberately, so the OAuth/OIDC implementation can be taken and
;; mounted on someone else's HTTP server.
;;
;; Modules are folders: <dimension>/<impl>/src is alias :<dimension>-<impl>
;; and jar synthigy-<dimension>-<impl>.jar. `check-layout!` refuses a folder
;; without its alias and an alias without its folder.

(defn impls [dim]
  (->> (.listFiles (java.io.File. ^String dim))
       (filter #(.isDirectory (java.io.File. ^java.io.File % "src")))
       (map #(.getName ^java.io.File %))
       sort))

(defn module-alias [dim impl] (keyword (str dim "-" impl)))

(defn aliases-of [dim] (mapv #(module-alias dim %) (impls dim)))

(defn adapter? [impl]
  (.isFile (java.io.File. (str "server/" impl "/src/synthigy/server.clj"))))

;; folders that exist but ship no jar: the compile witness, broken CockroachDB,
;; the adapters no bundle uses, and DuckDB (source only — its native driver
;; would otherwise count as shared and land in core.jar)
(def unshipped #{:engine-stub :engine-cockroach :server-pedestal :server-undertow :server-ring-jetty
                 :audit-duckdb :log-duckdb :lib-duckdb})

(defn shipped [dim] (vec (remove unshipped (aliases-of dim))))

(def db-aliases (shipped "engine"))
(def audit-aliases (shipped "audit"))
(def log-aliases (shipped "log"))
;; plain libraries a module may need on its classpath but never ships
(def lib-aliases (shipped "lib"))
(def oauth-aliases [:server-oauth])
(def server-aliases [:server-oauth :server-core])
(def leaf-aliases (into [:server-console]
                        (remove unshipped (map #(module-alias "server" %) (filter adapter? (impls "server"))))))
(def core-aliases [])

(defn check-layout!
  "Every module folder has its deps.edn alias pointing at it, and every dimension alias has its folder."
  []
  (let [declared (:aliases (clojure.edn/read-string (slurp "deps.edn")))
        folders  (into {} (for [dim ["engine" "audit" "log" "lib" "server"]
                                impl (impls dim)]
                            [(module-alias dim impl) (str dim "/" impl "/src")]))
        dim-alias? #(re-matches #"(engine|audit|log|lib|server)-.+" (name %))
        names? (fn [a path]
                 (or (some #{path} (get-in declared [a :extra-paths]))
                     (some #(= (str/replace path #"/src$" "") (:local/root %))
                           (vals (get-in declared [a :extra-deps])))))
        errors (concat
                (for [[a path] folders :when (not (names? a path))]
                  (str path " has no alias " a " naming it"))
                (for [a (keys declared)
                      :when (and (dim-alias? a) (not (folders a)))]
                  (str a " names no <dimension>/<impl>/src folder")))]
    (when (seq errors)
      (throw (ex-info (str "module layout: " (str/join "; " errors)) {:errors (vec errors)})))))

(defn check-layout [_]
  (check-layout!)
  (println "module layout ok:" (pr-str {:engine db-aliases :audit audit-aliases :log log-aliases
                                        :lib lib-aliases :server (into server-aliases leaf-aliases)})))

;; Each dimension's modules ship one shadowed namespace set: the engine
;; modules synthigy.core + synthigy.iam.audit, the audit modules synthigy.audit,
;; the log-store modules synthigy.log.store. Core requires all three, so its
;; COMPILE classpath carries a witness for each (engine/stub and the memory
;; modules); their classes belong to the module, never to core.
(def db-shadowed ["synthigy/core" "synthigy/iam/audit"])
(def audit-shadowed ["synthigy/audit"])
(def log-shadowed ["synthigy/log/store"])
(def shadowed (vec (concat db-shadowed audit-shadowed log-shadowed)))
(def witness-aliases [:engine-stub :audit-memory :log-memory])

(defn dimension-of [alias']
  (cond (some #{alias'} db-aliases)    :db
        (some #{alias'} audit-aliases) :audit
        (some #{alias'} log-aliases)   :log))

(defn witnesses-for
  "The compile witnesses for `alias'`: every dimension's except its own."
  [alias']
  (case (dimension-of alias')
    :db    [:audit-memory :log-memory]
    :audit [:engine-stub :log-memory]
    :log   [:engine-stub :audit-memory]
    witness-aliases))

(defn shadowed-excludes [ps]
  (mapv #(str % "(?:__init\\.class|\\$.*\\.class|\\.clj)") ps))

(defn dirs-of [basis]
  (filter #(.isDirectory (java.io.File. ^String %)) (keys (:classpath basis))))

(defn nses-in [dirs]
  (vec (remove no-aot (mapcat #(find/find-namespaces-in-dir (java.io.File. ^String %)) dirs))))

(defn munge-ns [ns-sym]
  (str/replace (clojure.lang.Compiler/munge (str ns-sym)) \. \/))

(defn class-owned?
  "Does this .class file belong to one of `owned-paths` (munged ns paths)?
   Namespace classes (`…__init`, `…$fn__1`) belong to their own ns path; type
   classes a defprotocol/defrecord/deftype emits (no `$`, no `__init`) to the
   ns directory they sit in — so synthigy/audit/protocol__init.class is
   synthigy.audit.protocol's, never synthigy.audit's."
  [owned-paths rel]
  (let [stem   (str/replace rel #"\.class$" "")
        base   (-> stem (str/split #"\$") first (str/replace #"__init$" ""))
        cut    (.lastIndexOf ^String base "/")
        owner  (if (or (str/includes? stem "$") (str/ends-with? stem "__init") (neg? cut))
                 base
                 (subs base 0 cut))]
    (boolean (owned-paths owner))))

(defn purge-foreign!
  "Keep only .class files this module's OWN namespaces compiled — including
   third-party libraries a shadowed/staged namespace transitively pulled in
   (clojure.jar ships its own .clj sources, and so do several deps here;
   *compile-files* AOT-recompiles anything it loads that isn't already
   flagged compiled in THIS process, regardless of an already-staged .class
   elsewhere on the classpath — a fresh compile-clj subprocess starts that
   flag from empty every time). Non-class resources (console's css/fonts/js)
   are untouched. A module's OWN mvn libs still arrive normally via :libs at
   uber time; this only removes the loose, redundant recompiled copies.
   Each anonymous fn also gets a new, non-reproducible $fn__NNNN suffix per
   process, so diffing by exact staged PATH (the old mechanism) silently
   missed these duplicates; filtering by owning NAMESPACE is suffix-proof."
  [dir owned]
  (let [owned-paths (set (map munge-ns owned))
        root (java.io.File. ^String dir)
        n (count (.getPath root))]
    (doseq [^java.io.File f (file-seq root)]
      (when (and (.isFile f)
                 (let [rel (subs (.getPath f) (inc n))]
                   (and (str/ends-with? rel ".class")
                        (not (class-owned? owned-paths rel)))))
        (.delete f)))))

(defn shadow-file?
  "A compiled artifact of a shadowed namespace: synthigy/core__init.class,
   synthigy/core$fn__1.class, ... — matched precisely so sibling namespaces
   (synthigy.core-something) are never caught."
  [rel]
  (some (fn [p] (or (= rel (str p "__init.class"))
                    (str/starts-with? rel (str p "$"))
                    (= rel (str p ".clj"))))
        shadowed))

(defn purge-shadowed!
  "Shadowed classes are driver-owned. Staging core's copies into a module
   would make it load the WITNESS driver's version (sqlite compiling against
   postgres's synthigy.core -> ClassNotFoundException on PGobject)."
  [dir]
  (let [root (java.io.File. ^String dir)
        n (count (.getPath root))]
    (doseq [^java.io.File f (file-seq root)]
      (when (and (.isFile f) (shadow-file? (subs (.getPath f) (inc n))))
        (.delete f)))))

(defn jar-entries [jar]
  (with-open [z (java.util.zip.ZipFile. (java.io.File. ^String jar))]
    (set (map #(.getName ^java.util.zip.ZipEntry %) (enumeration-seq (.entries z))))))

(defn build-module
  "One module jar: only this alias's own dirs and libs. `staged-classes`
   (one dir, or several — the server unit stages core AND is itself staged
   into httpkit/console) is copied into the class dir first so a shadowed
   namespace this module needs (synthigy.iam -> synthigy.iam.audit, say)
   resolves during compilation without ending up shipped in this jar.
   `compile-basis` resolves requires during THIS compile step (it may carry
   :engine-stub for a module with no db/obs of its own — see `modules`); `basis`
   is what the shipped jar's own deps are selected from, kept stub-free so
   a compile-time witness never becomes a runtime dependency.
   `purge-foreign!` (not a staged-path diff, see its docstring) is what
   keeps staged/transitively-recompiled classes out of the final jar."
  [alias' dirs libs compile-basis basis staged-classes]
  (let [cd (format "target/classes-%s" (name alias'))
        jar (format "target/synthigy-%s.jar" (name alias'))
        ;; a module ships only its own dimension's shadowed namespaces; oauth,
        ;; server, httpkit, console and the libs ship none
        foreign (case (dimension-of alias')
                  :db    (into audit-shadowed log-shadowed)
                  :audit (into db-shadowed log-shadowed)
                  :log   (into db-shadowed audit-shadowed)
                  shadowed)
        owned (nses-in dirs)]
    (b/delete {:path cd})
    (doseq [sc (if (string? staged-classes) [staged-classes] staged-classes)]
      (b/copy-dir {:src-dirs [sc] :target-dir cd}))
    (purge-shadowed! cd)
    ;; Resources are copied verbatim, never recompiled, so unlike classes they
    ;; keep stable paths — an exact-path snapshot IS the right diff for them,
    ;; and it's the only thing that stops every layer re-shipping the layer
    ;; below's resources (core's oauth login pages, dataset/xsql exports).
    (let [staged-resources (into #{} (comp (filter #(.isFile ^java.io.File %))
                                           (map #(.getPath ^java.io.File %))
                                           (remove #(str/ends-with? % ".class")))
                                 (file-seq (java.io.File. cd)))]
      (doseq [d dirs] (b/copy-dir {:src-dirs [d] :target-dir cd :ignores [#".*\.clj[cs]?$"]}))
      (b/compile-clj {:basis compile-basis :class-dir cd :ns-compile owned})
      (doseq [^java.io.File f (file-seq (java.io.File. cd))]
        (when (and (.isFile f) (staged-resources (.getPath f))) (.delete f))))
    ;; purge-foreign! alone is enough here: a stub-compiled shadowed class
    ;; from a bare `synthigy.core` require (ops.clj, httpkit's server.clj)
    ;; is NOT in `owned` for anything but a db alias, so it's caught same as
    ;; any other foreign class. A second unconditional purge-shadowed! here
    ;; would be wrong — it doesn't know "own vs foreign" and would delete a
    ;; db alias's OWN legitimate synthigy.core right back out again.
    (purge-foreign! cd owned)
    (b/uber {:class-dir cd :uber-file jar
             :basis (update basis :libs select-keys libs)
             :exclude (into [never-ship "(?:synthigy|patcho)/.*\\.clj[cs]?"]
                            (shadowed-excludes foreign))})
    (println "Built" jar)
    jar))

(defn assert-ownership! [_]
  (let [audit #{"synthigy/audit"}]
    (assert (class-owned? audit "synthigy/audit__init.class"))
    (assert (class-owned? audit "synthigy/audit$fn__12.class"))
    (assert (class-owned? audit "synthigy/audit/Thing.class"))
    (assert (not (class-owned? audit "synthigy/audit/protocol__init.class")))
    (assert (not (class-owned? audit "synthigy/audit/protocol$fn__7.class")))
    (assert (not (class-owned? audit "synthigy/audit/protocol/AuditProvider.class")))
    (assert (class-owned? #{"synthigy/audit/protocol"} "synthigy/audit/protocol/AuditProvider.class"))))

(defn modules [_]
  (check-layout!)
  (assert-ownership! nil)
  (let [core-basis (b/create-basis {:aliases core-aliases})
        core-dirs  (dirs-of core-basis)
        core-libs  (set (keys (:libs core-basis)))
        lib-dirs (set (mapcat #(dirs-of (b/create-basis {:aliases [%]})) lib-aliases))
        entry (fn [a]
                (let [bs  (b/create-basis {:aliases (conj core-aliases a)})
                      cbs (b/create-basis {:aliases (into core-aliases (cons a (witnesses-for a)))})]
                  {:basis bs :compile-basis cbs
                   :dirs (remove (into (set core-dirs) (if (some #{a} lib-aliases) #{} lib-dirs)) (dirs-of bs))
                   :libs (remove core-libs (keys (:libs bs)))}))
        db+obs (into {} (for [a (concat db-aliases audit-aliases log-aliases lib-aliases)] [a (entry a)]))
        ;; each layer subtracts every layer below it, so a module ships only
        ;; what it added and nothing is compiled into two jars
        oauth-basis (b/create-basis {:aliases (into core-aliases oauth-aliases)})
        oauth-compile-basis (b/create-basis {:aliases (into core-aliases (concat oauth-aliases witness-aliases))})
        oauth-dirs (remove (set core-dirs) (dirs-of oauth-basis))
        oauth-libs (remove core-libs (keys (:libs oauth-basis)))
        below-server (set (concat core-dirs oauth-dirs))
        server-basis (b/create-basis {:aliases (into core-aliases server-aliases)})
        server-compile-basis (b/create-basis {:aliases (into core-aliases (concat server-aliases witness-aliases))})
        server-dirs (remove below-server (dirs-of server-basis))
        server-libs (remove (set (concat core-libs oauth-libs)) (keys (:libs server-basis)))
        excl (set (concat core-dirs oauth-dirs server-dirs))
        leaf (into {} (for [a leaf-aliases]
                        (let [bs  (b/create-basis {:aliases (into core-aliases (conj server-aliases a))})
                              cbs (b/create-basis {:aliases (into core-aliases (concat server-aliases [a] witness-aliases))})]
                          [a {:basis bs :compile-basis cbs
                              :dirs (remove excl (dirs-of bs))
                              :libs (remove (set (concat core-libs oauth-libs server-libs)) (keys (:libs bs)))}])))
        ;; a lib two db/obs drivers both need (HikariCP: postgres+sqlite)
        ;; would be duplicated on the classpath — it belongs to core instead.
        ;; NOT extended to the server unit / httpkit / console: those are
        ;; never interchangeable picks, so pushing a shared lib of theirs
        ;; (hiccup, say) into core would put a UI-rendering dep back into the
        ;; engine artifact — exactly what this split exists to avoid.
        shared (set (for [[l n] (frequencies (mapcat (comp :libs val) db+obs)) :when (> n 1)] l))
        ;; the witnesses supply every shadowed namespace with no driver attached;
        ;; compiling against a real backend instead would put its shadowed
        ;; classes in core's class dir, one exclude away from shipping
        witness (b/create-basis {:aliases (into core-aliases witness-aliases)})
        ;; the staging chain: each cd is cumulative, so a module stages only
        ;; the layer directly below it and gets everything under it for free
        core-cd "target/classes-core"
        oauth-cd "target/classes-server-oauth"
        server-cd "target/classes-server-core"]
    (b/delete {:path core-cd})
    (doseq [d core-dirs] (b/copy-dir {:src-dirs [d] :target-dir core-cd :ignores [#".*\.clj[cs]?$"]}))
    (b/compile-clj {:basis witness :class-dir core-cd :ns-compile (nses-in core-dirs)})
    (doseq [p shadowed]
      (b/delete {:path (str core-cd "/" p ".clj")}))
    (b/uber {:class-dir core-cd :uber-file "target/synthigy-core.jar"
             :basis (update core-basis :libs
                            (fn [ls] (merge (select-keys ls core-libs)
                                            (select-keys (into {} (mapcat (comp :libs :basis val) db+obs)) shared))))
             :exclude (into [never-ship "(?:synthigy|patcho)/.*\\.clj[cs]?"] (shadowed-excludes shadowed))})
    (println "Built target/synthigy-core.jar")
    (let [core-entries (jar-entries "target/synthigy-core.jar")]
      (doseq [[a {:keys [dirs libs compile-basis basis]}] db+obs]
        (build-module a dirs (remove shared libs) compile-basis basis core-cd))
      (build-module :server-oauth oauth-dirs oauth-libs oauth-compile-basis oauth-basis core-cd)
      (build-module :server-core server-dirs server-libs server-compile-basis server-basis oauth-cd)
      (doseq [[a {:keys [dirs libs compile-basis basis]}] leaf]
        (build-module a dirs libs compile-basis basis server-cd))
      (println "\nshared->core:" (pr-str (vec shared)))
      (doseq [[a {:keys [libs]}] db+obs]
        (println (format "  %-16s libs: %s" (name a) (pr-str (vec (remove shared libs))))))
      (println (format "  %-16s libs: %s" "server-oauth" (pr-str (vec oauth-libs))))
      (println (format "  %-16s libs: %s" "server-core" (pr-str (vec server-libs))))
      (doseq [[a {:keys [libs]}] leaf]
        (println (format "  %-16s libs: %s" (name a) (pr-str (vec libs)))))
      (println "core entries:" (count core-entries)))))

;; --- Clojars: one fused SOURCE jar per product bundle ----------------------
;; SYNTHIGY_COMBO picks the folders, SYNTHIGY_BUNDLE names the artifact
;; (com.synthigy/<bundle>-bundle), SYNTHIGY_VERSION its version, and
;; SYNTHIGY_TOOLING_JS the built web components. Run from the exported pub/core
;; tree, never edge. See docs/plans/PLAN-CLOJARS-PUBLISHING.md.

(def patcho-version "0.4.4")

(defn bundle-params []
  (let [env #(or (System/getenv %) (throw (ex-info (str % " is required") {})))
        bundle (env "SYNTHIGY_BUNDLE")
        version (env "SYNTHIGY_VERSION")]
    {:bundle bundle
     :version version
     :class-dir (str "target/bundle-classes-" bundle)
     :lib (symbol "com.synthigy" (str bundle "-bundle"))
     :jar-file (format "target/%s-bundle-%s.jar" bundle version)}))

(defn bundle-deps
  "Maven deps the bundle's pom declares: top-level libs, the console's own deps, and patcho from Clojars."
  [basis]
  (let [libs (:libs basis)
        local (set (filter #(:local/root (libs %)) (keys libs)))
        console-only? #(and (seq %) (every? #{'dev.synthigy/console} %))]
    (into {'dev.gersak/patcho {:mvn/version patcho-version}}
          (for [[lib {:keys [mvn/version dependents]}] libs
                :when (and version (not (local lib))
                           (or (empty? dependents) (console-only? dependents)))]
            [lib {:mvn/version version}]))))

(defn clojars-bundle [_]
  (let [{:keys [lib version jar-file class-dir]} (bundle-params)
        tooling-js (or (System/getenv "SYNTHIGY_TOOLING_JS")
                       (throw (ex-info "SYNTHIGY_TOOLING_JS is required" {})))
        basis (b/create-basis {:aliases aliases})
        patcho (set (get-in basis [:libs 'dev.gersak/patcho :paths]))
        dirs (remove patcho (dirs-of basis))]
    (b/delete {:path class-dir})
    (b/copy-dir {:src-dirs (vec dirs) :target-dir class-dir})
    (b/delete {:path (str class-dir "/public/js")})
    (b/copy-file {:src tooling-js :target (str class-dir "/synthigy/console/js/tooling.js")})
    (doseq [f license-files]
      (b/copy-file {:src f :target (str class-dir "/synthigy/" f)}))
    (b/write-file {:path (str class-dir "/synthigy/VERSION") :string version})
    (b/write-pom {:class-dir class-dir :lib lib :version version
                  :basis (b/create-basis {:project {:deps (bundle-deps basis) :paths []}})
                  :repos {"clojars" {:url "https://repo.clojars.org/"}}
                  :scm {:url "https://github.com/synthigy/synthigy"
                        :connection "scm:git:https://github.com/synthigy/synthigy.git"
                        :tag (str "v" version)}
                  :pom-data [[:description "Synthigy — model-driven IAM and data platform, one dependency."]
                             [:url "https://github.com/synthigy/synthigy"]
                             [:licenses
                              [:license
                               [:name "Elastic License 2.0"]
                               [:url "https://www.elastic.co/licensing/elastic-license"]]]]})
    (b/jar {:class-dir class-dir :jar-file jar-file})
    (println "Built" jar-file)))

(defn deploy-bundle
  "Push the bundle to Clojars. Needs CLOJARS_USERNAME + CLOJARS_PASSWORD (a deploy token)."
  [_]
  (let [{:keys [lib jar-file class-dir]} (bundle-params)]
    (dd/deploy
     {:installer :remote :artifact (b/resolve-path jar-file)
      :pom-file (b/pom-path {:lib lib :class-dir class-dir})})))

(defn install-bundle
  "Install the bundle into the local ~/.m2 for hand testing."
  [_]
  (let [{:keys [lib jar-file class-dir]} (bundle-params)]
    (dd/deploy {:installer :local :artifact (b/resolve-path jar-file)
                :pom-file (b/pom-path {:lib lib :class-dir class-dir})})))
