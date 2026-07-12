(ns build
  "Uberjar for a chosen backend combo. NO AOT — sources are bundled and the
   server is launched via clojure.main, so packaging never boots the server
   (avoids side-effecting namespace loads). Combo via SYNTHIGY_COMBO env,
   e.g. SYNTHIGY_COMBO=postgres:httpkit (default)."
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

(def combo (or (System/getenv "SYNTHIGY_COMBO") "postgres:httpkit"))
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
    (doseq [d dirs] (b/copy-dir {:src-dirs [d] :target-dir class-dir}))
    (b/uber {:class-dir class-dir :uber-file uber-file :basis basis})
    (println "Built" uber-file)))
