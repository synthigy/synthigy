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

(ns synthigy.server.console.pages.encryption
  (:require
   [clojure.string :as str]
   [synthigy.dataset.encryption :as denc]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.console.widgets :as widgets]))

(def ^:private ladder
  [[:operator "Operator key" "SYNTHIGY_ENCRYPTION_MASTER_KEY, held by whoever provisions this deployment."]
   [:federated "Federated KMS" "Vault Transit or a webhook to your own KMS/HSM — the root key never enters this process."]])

(defn custody-rung
  "Which ladder rung `provider-tag` (encryption-status's :provider) is on."
  [provider-tag]
  (cond
    (nil? provider-tag) nil
    (str/starts-with? provider-tag "default") :operator
    :else :federated))

(defn custody-ladder
  [active-rung]
  [:ol.console-ladder
   (for [[rung label hint] ladder]
     [:li {:class (when (= rung active-rung) "active")}
      (icon/icon (if (= rung active-rung) :check :minus) {:size "13"})
      [:div [:div.console-ladder-label label] [:p.console-field-hint hint]]])])

(defn custody-tile
  [{:keys [provider]}]
  (let [rung (custody-rung provider)]
    (widgets/notice
     (if (= :federated rung)
       [:ok (str "Encryption custody: Federated KMS (" provider "). "
                 "The root key never enters this process.")]
       [:warn (str "Encryption custody: Operator local key. Protects a stolen "
                   "database dump, not a compromised host. Migrate to a "
                   "federated KMS before production.")])
     true)))

(def ^:private source->rung
  "configured-source's vocabulary (what boot precedence would pick) onto the
   ladder's (what's actually running) — same axis, kept as two vocabularies
   because configured-source distinguishes HOW a Local provider was reached
   (env var vs none), which the ladder rung already does via custody-rung.
   :default means no custody is configured at all — a next boot would refuse
   to start, not fall back to anything."
  {:vault :federated :webhook :federated :manual :operator})

(defn drift-notice
  "Notice when env config and the running process disagree — by custody rung or by operator-key value."
  [{:keys [provider]}]
  (let [configured-source (denc/configured-source)
        configured        (source->rung configured-source)
        active            (custody-rung provider)]
    (cond
      (nil? configured)
      (widgets/notice
       [:warn (str "No encryption custody is configured in the current environment — "
                   "SYNTHIGY_ENCRYPTION_MASTER_KEY is unset and no Vault/webhook config "
                   "is present. This deployment WILL NOT BOOT again until one is set.")]
       true)

      (and provider (not= configured active))
      (widgets/notice
       [:warn (str "Current env config would select " (name configured) " on next "
                   "boot, but this process is running under " (name active) ". "
                   "Restart to apply, or this is a live migrate-provider! that "
                   "hasn't been matched with a persistent env change yet.")]
       true)

      (false? (denc/env-master-key-matches?))
      (widgets/notice
       [:warn (str "SYNTHIGY_ENCRYPTION_MASTER_KEY no longer matches the keys this "
                   "process is running under — the master key was rotated here, but "
                   "the environment still holds the old one. This deployment WILL NOT "
                   "BOOT until you set the new value everywhere it starts.")]
       true))))

(defn dek-rows
  []
  (->> (denc/db-deks)
       (map (fn [{id :__deks/id active :__deks/active
                  wp :__deks/wrap_provider created :__deks/created_at}]
              {:id id :active active :wrap-provider wp :created created}))
       (sort-by :id)
       reverse))

(defn panel
  []
  (let [{:keys [provider deks_count master_key_present] :as status} (denc/encryption-status)]
    [:div#encryption-panel.console-panel
     [:div.console-system-head
      [:div.console-settings-head (icon/icon :key {:size "13"}) "Key custody"]
      [:div.console-head-actions

       [:ty-button {:type "button" :size "xs" :appearance "outlined" :muted true
                    :flavor "warning"
                    (keyword "data-on:click")
                    (str "$confirmAction = '/console/encryption/rotate-dek'; "
                         "$confirmTarget = 'dek'; $confirmVerb = 'Rotate encryption key'; "
                         "$confirmWhat = '"
                         "Activates a fresh Data Encryption Key. New and updated values "
                         "are encrypted under it; everything already written stays "
                         "readable under its previous key and migrates as it is written."
                         "'")}
        (icon/icon :activity {:size "11" :slot "start"})
        "Rotate encryption key"]]]
     [:ty-scroll-container {:custom-scrollbar true :shadow true}
      [:div.console-form-body
       (custody-tile status)
       (drift-notice status)
       [:div.console-guide
        [:p "Moving custody lives in the "
         [:strong "operator console"]
         " — run " [:code "synthigy console"] " on the host that runs this "
         "deployment. A migration is three things that have to happen "
         "together: rewrap every data key, change the environment, restart "
         "onto it. Only the operator console can do all three; this page "
         "could rewrap the keys but neither write the env file nor restart, "
         "which left the next boot unable to start."]
        [:p "Rotation and the key table stay here, where the change can be "
         "attributed to a signed-in user."]]
       (custody-ladder (custody-rung provider))
       [:table.console-table
        [:thead [:tr [:th "DEK"] [:th "Status"] [:th "Wrap provider"] [:th "Created"]]]
        [:tbody
         (for [{:keys [id active wrap-provider created]} (dek-rows)]
           [:tr {:id (str "dek-" id)}
            [:td.mono (str id)]
            [:td (if active
                   [:ty-tag {:size "sm" :flavor "success"} "Active"]
                   [:ty-tag {:size "sm" :flavor "neutral-"} "Retired"])]
            [:td.mono (or wrap-provider "—")]
            [:td.mono (or (widgets/ts created) "—")]])]]
       [:p.console-field-hint
        (str deks_count " DEK" (when (not= 1 deks_count) "s") " total. Retired keys "
             "stay loaded so everything written under them remains readable — "
             "values re-encrypt under the active key as they are written. "
             (if master_key_present
               "The raw master key currently sits in this process' memory (Local provider)."
               "The root key is held outside this process."))]]]]))


(defn render
  [request & [notice]]
  (ui/admin-shell
   {:title "Encryption"
    :user (:console/principal request)
    :uri (:uri request)
    :confirm "/console/encryption/rotate-dek"
    :session (:console/session request)
    :search :none
    :body
    [:div.console-page
     [:div.console-page-head
      [:div.console-eyebrow "System"]
      [:h1.console-title "Encryption"]
      [:p.console-subtitle
       (str "Every deployment starts encrypted — an operator-supplied key, "
            "or a federated KMS. This shows which one is protecting your "
            "data and how many keys are in play.")]]
     (widgets/notice notice true)
     (panel)]}))
