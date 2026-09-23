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

(ns build
  "Uberjar for a chosen backend combo. Combo via SYNTHIGY_COMBO env,
   e.g. SYNTHIGY_COMBO=postgres:httpkit (default). Default build bundles
   sources (no AOT). SYNTHIGY_AOT=true compiles every local namespace and
   strips local .clj/.cljc/.cljs from the jar — bytecode-only service
   artifact for GitHub releases; source publishing is Clojars, not this."
  (:require [clojure.tools.build.api :as b]
            [clojure.tools.namespace.find :as find]
            [clojure.string :as str]))

;; test-only namespaces whose deps aren't on the shipped combos' classpath
(def no-aot '#{synthigy.oauth.client})

;; resources/public/js is gitignored frontend build output (data console,
;; modeler — a separate project) that nothing in core serves; it must never
;; reach a jar, least of all its .js.map files, which embed .cljs source.
;; Excluded at b/uber, which matches full paths — copy-dir :ignores are
;; matched against the file NAME, so a directory pattern silently misses.
(def never-ship "public/js/.*")

(def combo (or (System/getenv "SYNTHIGY_COMBO") "postgres:httpkit"))
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
;;   core -> oauth -> server -> {httpkit, console}
;;
;; Each module is compiled with the PREVIOUS layer's class dir staged in, and
;; ships only what it added (`purge-foreign!`). oauth depends on core alone —
;; deliberately, so the OAuth/OIDC implementation can be taken and mounted on
;; someone else's HTTP server. httpkit and console both sit on server but not
;; on each other.
(def oauth-aliases [:oauth])
(def server-aliases [:oauth :server])
(def leaf-aliases [:httpkit :console])
(def core-aliases [])
(def db-aliases [:postgres :sqlite])
(def obs-aliases [:duckdb :clickhouse])

;; postgres/sqlite/cockroach each provide synthigy.core AND synthigy.iam.audit;
;; duckdb/clickhouse each provide synthigy.observability. Core requires the db
;; pair (iam.clj) so something must supply them on core's COMPILE
;; classpath — that is exactly what dummy/src (:stub) exists for. Their
;; classes belong to the driver module, never to core. Observability needs no
;; witness: supervisor's require is guarded and topics.clj only names it as a
;; string. A module ships only ITS OWN shadowed namespaces.
(def db-shadowed ["synthigy/core" "synthigy/iam/audit"])
(def obs-shadowed ["synthigy/observability"])
(def shadowed (into db-shadowed obs-shadowed))

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
   Clojure emits two shapes and BOTH must be kept: namespace classes at the
   ns path itself (synthigy/iam__init.class, synthigy/iam$fn__123.class), and
   the type classes a defprotocol/defrecord/deftype emits one segment INSIDE
   the namespace directory (synthigy/observability/TransportOps.class belongs
   to synthigy.observability). Missing the second shape drops the protocol
   interface and the module dies at load with ClassNotFoundException — only
   an actual composed-classpath boot catches it, not jar listings."
  [owned-paths rel]
  (let [base (-> rel
                 (str/replace #"\.class$" "")
                 (str/split #"\$")
                 first
                 (str/replace #"__init$" ""))
        cut (.lastIndexOf ^String base "/")
        parent (when (pos? cut) (subs base 0 cut))]
    (boolean (or (owned-paths base)
                 (and parent (owned-paths parent))))))

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
   :stub for a module with no db/obs of its own — see `modules`); `basis`
   is what the shipped jar's own deps are selected from, kept stub-free so
   a compile-time witness never becomes a runtime dependency.
   `purge-foreign!` (not a staged-path diff, see its docstring) is what
   keeps staged/transitively-recompiled classes out of the final jar."
  [alias' dirs libs compile-basis basis staged-classes]
  (let [cd (format "target/classes-%s" (name alias'))
        jar (format "target/synthigy-%s.jar" (name alias'))
        ;; a db module must never ship synthigy.observability, an obs module
        ;; never synthigy.core, and anything else (oauth, server, httpkit,
        ;; console) ships neither — one shadowed copy per classpath
        foreign (cond (some #{alias'} db-aliases) obs-shadowed
                      (some #{alias'} obs-aliases) db-shadowed
                      :else shadowed)
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

(defn modules [_]
  (let [core-basis (b/create-basis {:aliases core-aliases})
        core-dirs  (dirs-of core-basis)
        core-libs  (set (keys (:libs core-basis)))
        entry (fn [a extra-aliases]
                (let [bs  (b/create-basis {:aliases (into core-aliases (conj extra-aliases a))})
                      cbs (b/create-basis {:aliases (into core-aliases (conj extra-aliases a :stub))})]
                  {:basis bs :compile-basis cbs
                   :dirs (remove (set core-dirs) (dirs-of bs))
                   :libs (remove core-libs (keys (:libs bs)))}))
        db+obs (into {} (for [a (concat db-aliases obs-aliases)] [a (entry a [])]))
        ;; each layer subtracts every layer below it, so a module ships only
        ;; what it added and nothing is compiled into two jars
        oauth-basis (b/create-basis {:aliases (into core-aliases oauth-aliases)})
        oauth-compile-basis (b/create-basis {:aliases (into core-aliases (conj oauth-aliases :stub))})
        oauth-dirs (remove (set core-dirs) (dirs-of oauth-basis))
        oauth-libs (remove core-libs (keys (:libs oauth-basis)))
        below-server (set (concat core-dirs oauth-dirs))
        server-basis (b/create-basis {:aliases (into core-aliases server-aliases)})
        server-compile-basis (b/create-basis {:aliases (into core-aliases (conj server-aliases :stub))})
        server-dirs (remove below-server (dirs-of server-basis))
        server-libs (remove (set (concat core-libs oauth-libs)) (keys (:libs server-basis)))
        excl (set (concat core-dirs oauth-dirs server-dirs))
        leaf (into {} (for [a leaf-aliases]
                        (let [bs  (b/create-basis {:aliases (into core-aliases (conj server-aliases a))})
                              cbs (b/create-basis {:aliases (into core-aliases (conj server-aliases a :stub))})]
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
        ;; :stub (dummy/src) supplies synthigy.core + synthigy.iam.audit with
        ;; no driver attached — built for exactly this. Compiling against a
        ;; real backend instead would put that driver's shadowed classes in
        ;; core's class dir, one exclude away from shipping.
        witness (b/create-basis {:aliases (conj core-aliases :stub)})
        ;; the staging chain: each cd is cumulative, so a module stages only
        ;; the layer directly below it and gets everything under it for free
        core-cd "target/classes-core"
        oauth-cd "target/classes-oauth"
        server-cd "target/classes-server"]
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
      (build-module :oauth oauth-dirs oauth-libs oauth-compile-basis oauth-basis core-cd)
      (build-module :server server-dirs server-libs server-compile-basis server-basis oauth-cd)
      (doseq [[a {:keys [dirs libs compile-basis basis]}] leaf]
        (build-module a dirs libs compile-basis basis server-cd))
      (println "\nshared->core:" (pr-str (vec shared)))
      (doseq [[a {:keys [libs]}] db+obs]
        (println (format "  %-12s libs: %s" (name a) (pr-str (vec (remove shared libs))))))
      (println (format "  %-12s libs: %s" "oauth" (pr-str (vec oauth-libs))))
      (println (format "  %-12s libs: %s" "server" (pr-str (vec server-libs))))
      (doseq [[a {:keys [libs]}] leaf]
        (println (format "  %-12s libs: %s" (name a) (pr-str (vec libs)))))
      (println "core entries:" (count core-entries)))))
