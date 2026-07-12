(ns synthigy.oauth.consent
  (:require
   [clojure.string :as str]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]
   [synthigy.dataset.sql.naming :as naming]))

(def -oidc-client- #uuid "0757bd93-7abf-45b4-8437-2841283edcba")
(def -oidc-session- #uuid "b2562198-0817-4508-a941-d898373298e5")
(def -oidc-scope- #uuid "eb03406a-9b0c-4d61-8f96-34e8aa04f13c")
(def -oidc-user- #uuid "edcab1db-ee6f-4744-bfea-447828893223")

;; Postgres-only: consent persistence uses to_regclass / now().

(defn create-consent-table
  []
  (sql/execute!
   [(str/join
     "\n"
     ["create table __oidc_user_consent ("
      (format "   \"user\" bigint not null references \"%s\"(_eid) on delete cascade," (naming/table *db* -oidc-user-))
      (format "   \"client\" bigint not null references \"%s\"(_eid) on delete cascade," (naming/table *db* -oidc-client-))
      (format "   \"scope\" bigint not null references \"%s\"(_eid) on delete cascade," (naming/table *db* -oidc-scope-))
      "   \"consent_at\" TIMESTAMP default now(),"
      "   unique (\"user\", \"client\", \"scope\")"
      ")"])]))

(defn delete-consent-table
  []
  (sql/execute! ["drop table __oidc_user_consent"]))

(defn consent-table-exists?
  []
  (let [[{result :to_regclass}] (sql/execute! ["SELECT to_regclass('public.__oidc_user_consent')"])]
    (some? result)))

(comment
  (create-consent-table)
  (delete-consent-table)
  (consent-table-exists?))

(defn user-provided-consent [user client scopes]
  (sql/execute!
   (into
    [(str/join
      "\n"
      [(format "WITH data (username, clientid, scopename) AS (VALUES %s" (str/join ", " (repeat (count scopes) "(?,?,?)")))
       "), ids AS ("
       "SELECT "
       (format "(SELECT _eid FROM \"%s\" WHERE name = d.username) AS user, " (naming/table *db* -oidc-user-))
       (format "(SELECT _eid FROM \"%s\" WHERE client_id = d.clientid) AS client, " (naming/table *db* -oidc-client-))
       (format "(SELECT _eid FROM \"%s\" WHERE name = d.scopename) AS scope" (naming/table *db* -oidc-scope-))
       "FROM data d"
       ") "
       "INSERT INTO __oidc_user_consent (user, client, scope)"
       "SELECT user, client, scope FROM ids"])]
    (map (fn [scope] [user client scope]) scopes))))

(comment
  (def scopes [100 200 300 400]))
