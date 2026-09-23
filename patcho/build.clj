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
  (:require
    [clojure.edn :as edn]
    [clojure.tools.build.api :as b]
    [deps-deploy.deps-deploy :as dd]))

(def versions
  (let [{:keys [out]} (b/process
                        {:command-args ["clj" "-X" "patcho.cli/versions" ":require" "patcho.patch"]
                         :out :capture})]
    (edn/read-string out)))
(def version (:dev.gersak/patcho versions))

(def target "target/classes")

(defn create-jar [_]
  (let [basis (b/create-basis {})
        jar-file (format "target/patcho-%s.jar" version)]
    (b/delete {:path "target"})
    (b/copy-dir {:src-dirs ["src"]
                 :target-dir target})
    (b/write-pom {:target target
                  :lib 'dev.gersak/patcho
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :scm {:url "https://github.com/gersak/patcho"
                        :connection "scm:git:git://github.com/gersak/patcho.git"
                        :developerConnection "scm:git:ssh://git@github.com/gersak/patcho.git"
                        :tag (str "v" version)}
                  :pom-data [[:description "Component versioning and lifecycle management for Clojure"]
                             [:url "https://github.com/gersak/patcho"]
                             [:licenses
                              [:license
                               [:name "MIT"]
                               [:url "https://opensource.org/licenses/MIT"]]]
                             [:developers
                              [:developer
                               [:name "Robert Gersak"]]]]})
    (b/jar {:class-dir target
            :jar-file jar-file})))

(defn release
  ([] (release nil))
  ([{t :test}]
   (create-jar nil)
   (let [jar-file (format "target/patcho-%s.jar" version)
         pom-file (str target "/pom.xml")
         installer (if t :local :remote)]
     (println "Deploying JAR:" jar-file)
     (dd/deploy {:installer installer
                 :sign-releases? false
                 :artifact jar-file
                 :pom-file pom-file}))))

(comment
  (def config-file "shadow-cljs.prod.edn")
  (release))
