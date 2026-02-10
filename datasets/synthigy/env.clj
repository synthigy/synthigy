(ns synthigy.env
  (:require
   [babashka.fs :as fs]
   [environ.core :refer [env]]))

(def home (str (fs/expand-home (env :synthigy-home "~/.synthigy"))))
(def pid (str home "/pid"))
(def admin-port (str home "/admin.port"))
(def log-dir (str (fs/absolutize (env :synthigy-log-dir (str home "/logs")))))
(def config-dir (str (fs/absolutize (env :synthigy-config-dir (str home "/config")))))
(def git-dir (str (fs/absolutize (env :synthigy-git-dir (str home "/git")))))

(def iam-root-url (env :synthigy-iam-root-url))

(def id-format
  "ID format preference: \"euuid\" or \"xid\".
  When set, system verifies stored format matches on startup."
  (env :synthigy-id-format))
