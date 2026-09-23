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

(ns synthigy.iam.connector.dev
  "REPL helpers for working with the credentials connector chain; not loaded in
   production paths."
  (:require
   [clojure.string :as str]
   [synthigy.iam.connector :as c]))

;; =============================================================================
;; Inspection
;; =============================================================================

(defn ellipsize [s n]
  (let [s (str s)]
    (if (> (count s) n)
      (str (subs s 0 (max 0 (- n 1))) "…")
      s)))

(defn col [v width]
  (let [s (ellipsize v width)]
    (apply str s (repeat (max 0 (- width (count s))) " "))))

(defn print-chain
  "Pretty-print the active connector chain as a table."
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
  "Show every connector type with a registered `verify-credentials` defmethod."
  []
  (->> (methods c/verify-credentials)
       keys
       (sort-by str)
       (run! (fn [k] (println " " k)))))

;; =============================================================================
;; Mutation shortcuts
;; =============================================================================

(defn seed-webhook!
  "Insert a webhook connector with sensible defaults. Returns the persisted row."
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
  "Insert (or re-enable) the local-database connector at a given priority."
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
  "Run the full chain against the given credentials and report what happened;
   doesn't create sessions or tokens."
  [username password]
  (let [user (c/authenticate {:username username :password password})]
    (if user
      (do
        (println "✅ Authenticated as" username)
        (println "   Local user :_eid =" (:_eid user))
        (println "   Local user :xid =" (:xid user))
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
