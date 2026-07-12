(ns synthigy.iam.connector.dev
  "REPL helpers for working with the credentials connector chain.

  Not loaded in production paths — `(require 'synthigy.iam.connector.dev)`
  when you want them. Convenience wrappers over the
  `synthigy.iam.connector` sugar API plus a tabular pretty-printer for
  inspecting the active chain."
  (:require
   [clojure.string :as str]
   [synthigy.iam.connector :as c]))

;; =============================================================================
;; Inspection
;; =============================================================================

(defn- ellipsize [s n]
  (let [s (str s)]
    (if (> (count s) n)
      (str (subs s 0 (max 0 (- n 1))) "…")
      s)))

(defn- col [v width]
  (let [s (ellipsize v width)]
    (apply str s (repeat (max 0 (- width (count s))) " "))))

(defn print-chain
  "Pretty-print the active connector chain as a table. Useful for sanity-
   checking ordering, enabled flags, domain filters, and types after edits.

   Example output:

     PRIORITY  TYPE              DOMAIN     ENABLED  NAME              XID
     -----------------------------------------------------------------------
     100       :webhook          corp.com   true     Corp AD           1a2b3c…
     500       :acme/mainframe              true     Mainframe         4d5e6f…
     1000      :database                    true     Local database    7g8h9i…"
  []
  (let [chain (c/list-chain)
        widths {:priority 8 :type 17 :domain 10 :enabled 8 :name 18 :xid 10}
        header (str (col "PRIORITY" (:priority widths)) "  "
                    (col "TYPE"     (:type widths))     "  "
                    (col "DOMAIN"   (:domain widths))   "  "
                    (col "ENABLED" (:enabled widths))   "  "
                    (col "NAME"     (:name widths))     "  "
                    (col "XID"      (:xid widths)))
        sep (apply str (repeat (count header) "-"))]
    (println header)
    (println sep)
    (doseq [c chain]
      (println (str (col (:priority c) (:priority widths)) "  "
                    (col (str (:type c)) (:type widths)) "  "
                    (col (or (:domain c) "") (:domain widths)) "  "
                    (col (:enabled c) (:enabled widths)) "  "
                    (col (:name c) (:name widths)) "  "
                    (col (:xid c) (:xid widths)))))
    (println sep)
    (println (count chain) "connector(s)")))

(defn list-types
  "Show every connector type with a registered `verify-credentials` defmethod.
   Helpful when debugging 'why isn't my custom :acme/foo connector firing?'"
  []
  (->> (methods c/verify-credentials)
       keys
       (sort-by str)
       (run! (fn [k] (println " " k)))))

;; =============================================================================
;; Mutation shortcuts
;; =============================================================================

(defn seed-webhook!
  "Insert a webhook connector with sensible defaults. Returns the persisted
   row.

   ```
   (dev/seed-webhook! \"https://auth.internal/verify\" \"shared-secret\")
   (dev/seed-webhook! \"https://auth.internal/verify\" \"shared-secret\" 100)
   ```"
  ([url secret] (seed-webhook! url secret 100))
  ([url secret priority]
   (c/save-connector!
    {:type :webhook
     :name (str "Webhook " (or (second (re-find #"://([^/]+)" url)) url))
     :priority priority
     :enabled true
     :url url
     :secret secret
     :timeout-ms 1500})))

(defn seed-database!
  "Insert (or re-enable) the local-database connector at a given priority.
   Most installs already have one seeded by the patch; this is for explicit
   re-creation in test scenarios."
  ([] (seed-database! 1000))
  ([priority]
   (c/save-connector!
    {:type :database
     :name "Local database"
     :priority priority
     :enabled true})))

(defn disable!
  "Toggle a connector off without deleting it (re-enable later with `enable!`)."
  [xid]
  (when-let [conn (c/find-connector xid)]
    (c/save-connector! (assoc conn :enabled false))))

(defn enable! [xid]
  (when-let [conn (c/find-connector xid)]
    (c/save-connector! (assoc conn :enabled true))))

(defn set-priority!
  "Reorder a connector. Lower priority runs first in the chain."
  [xid priority]
  (when-let [conn (c/find-connector xid)]
    (c/save-connector! (assoc conn :priority priority))))

(defn set-domain!
  "Set or clear (`nil`) the email-domain filter on a connector."
  [xid domain]
  (when-let [conn (c/find-connector xid)]
    (c/save-connector! (assoc conn :domain domain))))

;; =============================================================================
;; Tryouts
;; =============================================================================

(defn try-authenticate
  "Run the full chain against the given credentials and report what happened.
   Doesn't create sessions or tokens — just exercises `connector/authenticate`
   and prints the result.

   ```
   (dev/try-authenticate \"alice\" \"correct-password\")
   ```"
  [username password]
  (let [user (c/authenticate {:username username :password password})]
    (if user
      (do
        (println "✅ Authenticated as" username)
        (println "   Local user :_eid =" (:_eid user))
        (println "   Local user :euuid =" (:euuid user))
        (println "   Active =" (:active user))
        user)
      (do
        (println "❌ Authentication denied for" username)
        nil))))

(comment
  ;; Typical workflow:
  (require '[synthigy.iam.connector.dev :as dev])

  (dev/print-chain)
  (dev/list-types)

  (def w (dev/seed-webhook! "https://auth.internal/verify" "secret123"))
  (dev/print-chain)

  (dev/disable! (:xid w))
  (dev/enable! (:xid w))
  (dev/set-priority! (:xid w) 50)
  (dev/set-domain! (:xid w) "corp.com")

  (dev/try-authenticate "alice" "alicepass"))
