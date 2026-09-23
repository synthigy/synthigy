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

(ns synthigy.supervisor.probe
  "Param-driven database probes for the operator console's Test button."
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [patcho.patch :as patch]
    [synthigy.dataset.encryption :as encryption]
    [synthigy.json :as json]
    [synthigy.supervisor :as supervisor]))

(defn driver? [class-name]
  (try (Class/forName class-name) true (catch Throwable _ false)))

(defn q1 [conn sql]
  (jdbc/execute-one! conn [sql] {:builder-fn rs/as-unqualified-lower-maps}))

(defn installed-versions [conn]
  (try
    (into {}
          (map (juxt :component :version))
          (jdbc/execute! conn ["select component, version from __component_versions__"]
                         {:builder-fn rs/as-unqualified-lower-maps}))
    (catch Exception _ {})))

(defn pending-upgrades [installed]
  (vec (for [[topic ver] (try (patch/available-versions) (catch Throwable _ {}))
             :let [inst (get installed (str topic))]
             ;; model topics read their version from the DB — nil in a probe
             ;; engine, and an unknown target is not a pending upgrade
             :when (and inst ver (not= inst ver))]
         (str topic " " inst " → " ver))))

(defn synthigy-verdict
  "Shared verdict for a database carrying a Synthigy layout. `legacy-aware?`
   is postgres-only: sqlite is structurally xid-only and never legacy."
  [installed legacy-aware?]
  (let [fmt     (get installed ":synthigy/id-format")
        pending (pending-upgrades installed)]
    (if (and legacy-aware? (not= fmt "xid"))
      {:ok true :kind "legacy" :versions installed
       :message (str "Connected. LEGACY euuid-keyed (EYWA) database — Synthigy is xid-native; "
                     "a one-time migration is required and the status page offers it after launch.")}
      {:ok true :kind "synthigy" :versions installed
       :message (if (seq pending)
                  (str "Connected. Existing Synthigy database — the engine will attach and patch forward ("
                       (str/join ", " pending) ").")
                  "Connected. Existing Synthigy database, fully current — the engine will attach to it.")})))

(defn dek-rows
  "__deks rows at `conn` (an ad-hoc probe connection, not db/*db*), or [] when
   the table doesn't exist. `select *`, not named columns: a pre-migration
   EYWA `__deks` predates the `wrap_provider`/`wrap_key_version` columns
   entirely (added later by `ensure-deks-columns!`, which hasn't run yet on
   a database this engine has never booted against) — naming the column
   would throw and get caught as `[]`, silently reporting no encrypted data
   on exactly the legacy dump this check exists to catch. A row with no such
   key reads as nil `:wrap_provider`, which `local-custody?` already treats
   as local — correct: an EYWA-era DEK has no other custody to be under."
  [conn]
  (try
    (jdbc/execute! conn ["select * from __deks"]
                   {:builder-fn rs/as-unqualified-lower-maps})
    (catch Exception _ [])))

(defn local-custody?
  [wrap-provider]
  (contains? #{nil "default" "manual"} wrap-provider))

(defn pgobject-value
  "Value of a raw JDBC PGobject, duck-typed (no compile-time driver dep) — a probe connection isn't a synthigy.db record, so db/Dialect (which dispatches on *db*) doesn't apply here."
  [v]
  (if (and v (= "org.postgresql.util.PGobject" (.getName (class v))))
    (.invoke (.getMethod (class v) "getValue" (into-array Class [])) v (into-array Object []))
    v))

(defn master-key-verified?
  "Does `master-key` unwrap `row`'s DEK? nil/blank never verifies."
  [row master-key]
  (boolean
   (and row (not (str/blank? master-key))
        (try
          (some? (encryption/unwrap-dek (encryption/->local-provider master-key)
                                         (json/read-str (pgobject-value (:dek row)))))
          (catch Throwable _ false)))))

(defn dek-verdict
  "Layers __deks custody onto a Synthigy verdict: local rows need a verified
   master key before launch, federated rows just need their own env config.
   Every branch APPENDS to the underlying verdict's message and keeps its
   `:kind` in `:layout` — a key problem is an EXTRA finding, not a
   replacement for what the layout probe already established. Overwriting
   the message meant an operator who mistyped the key stopped being told
   the database was legacy euuid at all."
  [verdict conn master-key]
  (let [rows  (dek-rows conn)
        local (first (filter (comp local-custody? :wrap_provider) rows))
        n     (count rows)
        keys* (str n " encrypted data key" (when (not= 1 n) "s"))]
    (cond
      (empty? rows) verdict

      (nil? local)
      (update verdict :message str " Custody is federated (" (:wrap_provider (first rows))
              ") — that provider's env config is needed at launch, not a master key.")

      (master-key-verified? local master-key)
      (assoc verdict :key_ok true :deks n
             :message (str (:message verdict) " Carries " keys* " — master key verified."))

      :else
      (assoc verdict :ok false :kind "master-key" :layout (:kind verdict)
             :needs_key true :deks n
             :message (str (:message verdict) " Carries " keys*
                           (if (str/blank? master-key)
                             " — enter this database's master key."
                             " — WRONG master key for this database."))))))

(defn probe-postgres [{:keys [host port dbname user password master_key]}]
  (if-not (driver? "org.postgresql.Driver")
    {:ok false :kind "no-driver"
     :message "This build carries no PostgreSQL driver — it cannot run (or probe) a PostgreSQL backend."}
    (try
      (with-open [conn (jdbc/get-connection
                         {:dbtype "postgresql"
                          :host (or (not-empty host) "localhost")
                          :port (or (some-> port str parse-long) 5432)
                          :dbname dbname
                          :user user
                          :password password
                          :loginTimeout 5
                          :connectTimeout 5})]
        (let [dv (:c (q1 conn "select count(*) c from information_schema.tables where table_schema='public' and table_name='dataset_version'"))
              nt (:c (q1 conn "select count(*) c from information_schema.tables where table_schema='public'"))]
          (cond
            (zero? (long nt))
            {:ok true :kind "empty"
             :message "Connected. Empty database — Synthigy will initialize it fresh on launch."}

            (zero? (long dv))
            {:ok true :kind "foreign"
             :message (str "Connected. The database holds " nt " tables but no Synthigy layout — "
                           "launching will deploy Synthigy alongside them. Prefer a dedicated database.")}

            :else (dek-verdict (synthigy-verdict (installed-versions conn) true) conn master_key))))
      (catch java.sql.SQLException e
        (condp contains? (.getSQLState e)
          #{"3D000"} {:ok false :kind "missing-db"
                      :message (str "Connected to the server, but database \"" dbname "\" does not exist.")}
          #{"28P01" "28000"} {:ok false :kind "auth-failed"
                              :message "Authentication failed — check the user and password."}
          ;; The driver refuses an empty password before it ever asks the
          ;; server, so this arrives with no SQLSTATE to match on — and its
          ;; own wording ("the password is an empty string") is not what an
          ;; operator who simply left the field blank needs to read.
          (if (str/includes? (str (.getMessage e)) "password is an empty string")
            {:ok false :kind "auth-failed"
             :message "This server requires a password and none was given."}
            {:ok false :kind "connect-failed" :message (.getMessage e)})))
      (catch Exception e
        {:ok false :kind "connect-failed" :message (or (ex-message e) (str (class e)))}))))

(defn sqlite-file? [f]
  (with-open [in (io/input-stream f)]
    (let [buf (byte-array 15)
          n   (.read in buf)]
      (and (= n 15) (= "SQLite format 3" (String. buf 0 15 "ISO-8859-1"))))))

(defn probe-sqlite [{:keys [path master_key]}]
  (if-not (driver? "org.sqlite.JDBC")
    {:ok false :kind "no-driver"
     :message "This build carries no SQLite driver — it cannot run (or probe) a SQLite backend."}
    (try
      (let [path (not-empty path)
            f    (some-> path io/file)]
        (cond
          (nil? path)
          {:ok true :kind "default" :message "No path set — the engine default applies."}

          (not (.exists f))
          {:ok true :kind "new"
           :message "No file there yet — the SQLite database is created on first boot."}

          (.isDirectory f)
          {:ok false :kind "invalid" :message "That path is a directory, not a file."}

          (not (sqlite-file? f))
          {:ok false :kind "not-sqlite" :message "File exists but is not a SQLite database."}

          :else
          (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname path})]
            (let [dv (:c (q1 conn "select count(*) c from sqlite_master where type='table' and name='dataset_version'"))
                  nt (:c (q1 conn "select count(*) c from sqlite_master where type='table'"))]
              (cond
                (zero? (long nt))
                {:ok true :kind "empty"
                 :message "Existing but empty SQLite file — Synthigy will initialize it on launch."}

                (zero? (long dv))
                {:ok true :kind "foreign"
                 :message (str "SQLite file holds " nt " tables but no Synthigy layout — launching "
                               "deploys Synthigy alongside them. Prefer a dedicated file.")}

                ;; sqlite is structurally xid-only — never legacy
                :else (dek-verdict (synthigy-verdict (installed-versions conn) false) conn master_key))))))
      (catch Exception e
        {:ok false :kind "probe-failed" :message (or (ex-message e) (str (class e)))}))))

(supervisor/register-db-probe! :postgres probe-postgres)
(supervisor/register-db-probe! :sqlite probe-sqlite)

(defn writable-file-target?
  "Missing `path`'s directory is creatable+writable — probed, not assumed."
  [path]
  (let [dir (or (.getParentFile (io/file path)) (io/file "."))]
    (try
      (.mkdirs dir)
      (let [probe (io/file dir ".synthigy-write-probe")]
        (spit probe "")
        (.delete probe)
        true)
      (catch Exception _ false))))

(defn duckdb-file? [f]
  (with-open [in (io/input-stream f)]
    (let [buf (byte-array 12)
          n   (.read in buf)]
      (and (= n 12) (= "DUCK" (String. buf 8 4 "ISO-8859-1"))))))

(defn probe-duckdb [{:keys [path]}]
  (try
    (let [path (not-empty path)
          f    (some-> path io/file)]
      (cond
        (nil? path)
        {:ok true :kind "default"
         :message "No path set — the engine default is IN-MEMORY: audit history is lost on restart."}

        (not (.exists f))
        (if (writable-file-target? path)
          {:ok true :kind "new" :message "Path is writable — the DuckDB store is created on first boot."}
          {:ok false :kind "not-writable" :message "Cannot write there — check the directory and permissions."})

        (.isDirectory f)
        {:ok false :kind "invalid" :message "That path is a directory, not a file."}

        (duckdb-file? f)
        {:ok true :kind "existing" :message "Existing DuckDB store — observability will attach to it."}

        :else
        {:ok false :kind "not-duckdb" :message "File exists but is not a DuckDB database."}))
    (catch Exception e
      {:ok false :kind "probe-failed" :message (or (ex-message e) (str (class e)))})))

(defn probe-clickhouse [{:keys [url user password]}]
  (let [url (some-> url str/trim (str/replace #"/+$" ""))]
    (if (str/blank? url)
      {:ok false :kind "missing"
       :message "ClickHouse needs its HTTP URL — the observability module refuses to start without it."}
      (try
        (let [client  (java.net.http.HttpClient/newBuilder)
              client  (.build (.connectTimeout client (java.time.Duration/ofSeconds 5)))
              builder (-> (java.net.http.HttpRequest/newBuilder)
                          (.uri (java.net.URI/create (str url "/ping")))
                          (.timeout (java.time.Duration/ofSeconds 5))
                          (.GET))
              builder (if (not-empty user)
                        (.header builder "Authorization"
                                 (str "Basic " (.encodeToString (java.util.Base64/getEncoder)
                                                                (.getBytes (str user ":" (or password "")) "UTF-8"))))
                        builder)
              resp    (.send client (.build builder)
                             (java.net.http.HttpResponse$BodyHandlers/ofString))]
          (if (and (= 200 (.statusCode resp)) (str/starts-with? (str/trim (.body resp)) "Ok"))
            {:ok true :kind "ok" :message "ClickHouse answered — observability will attach to it."}
            {:ok false :kind "bad-answer"
             :message (str "ClickHouse answered " (.statusCode resp) " \""
                           (str/trim (subs (str (.body resp)) 0 (min 120 (count (str (.body resp))))))
                           "\" — not a healthy /ping.")}))
        (catch Exception e
          {:ok false :kind "unreachable"
           :message (str "Could not reach ClickHouse: " (or (ex-message e) (str (class e))))})))))

(supervisor/register-method! "obs.probe"
  (fn [{:keys [obs] :as params}]
    (case obs
      "none"       {:ok true :kind "none" :message "Observability disabled — nothing to test."}
      "clickhouse" (probe-clickhouse params)
      (probe-duckdb params))))
