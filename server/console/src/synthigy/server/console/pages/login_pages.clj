;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.pages.login-pages
  (:require
   [hiccup2.core :refer [raw]]
   [synthigy.json :as json]
   [synthigy.oauth.page.custom :as custom]
   [synthigy.oauth.page.error :as error-page]
   [synthigy.oauth.onboarding :as onboarding]
   [synthigy.oauth.page.login :as login-page]
   [synthigy.oauth.page.status :as status-page]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.console.widgets :as widgets]))

(def base "/console/custom-pages")

(defn action-url [verb {:keys [name version]}]
  (str base "/" verb "?name=" name "&version=" version))

(defn preview-url [{:keys [name version]}]
  (str base "/preview/" name "/" version "/"))

(defn confirm-opener [verb what url]
  (str "$confirmAction = " (json/write-str url) "; "
       "$confirmTarget = 'login-page'; $confirmVerb = " (json/write-str verb) "; "
       "$confirmWhat = " (json/write-str what)))

(defn icon-action [attrs glyph tip]
  [:ty-button (merge {:type "button" :size "xs" :appearance "ghost" :action true} attrs)
   (icon/icon glyph {:size "13"})
   [:ty-tooltip {:placement "top"} tip]])

(defn row-actions [{:keys [name version active] :as row}]
  [:div.console-row-actions
   (icon-action {:onclick (str "window.open(" (json/write-str (preview-url row)) ",'_blank','noopener')")}
                :eye "Preview")
   (icon-action {:onclick (str "location.href=" (json/write-str (action-url "export" row)))}
                :download "Download zip")
   (if active
     (icon-action {:flavor "danger"
                   (keyword "data-on:click")
                   (confirm-opener "Stop serving"
                                   (str "Clients using \"" name "\" fall back to "
                                        (if (= custom/default-name name)
                                          "the built-in login page."
                                          "the default bundle or the built-in login page."))
                                   (action-url "deactivate" row))}
                  :square "Stop serving")
     (icon-action {:flavor "success"
                   (keyword "data-on:click")
                   (confirm-opener "Activate"
                                   (str "Serve version " version " of \"" name "\" at "
                                        (custom/page-url name) " from now on.")
                                   (action-url "activate" row))}
                  :play "Activate"))
   (icon-action {:flavor "danger" :muted true
                 (keyword "data-on:click")
                 (confirm-opener "Delete"
                                 (str "Delete version " version " of \"" name "\"."
                                      (when active " It is being served right now."))
                                 (action-url "delete" row))}
                :trash-2 "Delete")])

(defn bundles-table [rows]
  [:div.console-panel.console-bundles
   [:div.console-system-head
    [:div.console-settings-head (icon/icon :layers {:size "13"}) "Bundles"]]
   (if (empty? rows)
     [:div.console-form-body
      [:p.console-field-hint "Nothing uploaded yet — every page is the built-in one."]]
     [:ty-scroll-container {:custom-scrollbar true :shadow true}
      [:table.console-table
       [:thead [:tr [:th "Name"] [:th "Version"] [:th "Status"] [:th "Uploaded"] [:th.num ""]]]
       [:tbody
        (for [{:keys [name version active size uploaded-at uploaded-by digest] :as row} rows]
          [:tr {:id (str "login-page-" name "-" version)}
           [:td.name name [:div.console-subtle.mono (subs digest 0 12)]]
           [:td.mono (str "v" version) [:div.console-subtle (format "%.1f KB" (/ size 1024.0))]]
           [:td (if active
                  [:ty-tag {:size "sm" :flavor "success"}
                   (icon/icon :check {:size "11" :slot "start"}) "Serving"]
                  [:ty-tag {:size "sm" :flavor "neutral-"} "Stored"])]
           [:td.mono (or (widgets/ts uploaded-at) "—")
            [:div.console-subtle (or uploaded-by "—")]]
           [:td.num (row-actions row)]])]]])])

(def upload-script
  "document.getElementById('login-page-upload').addEventListener('click', async () => {
     const f = document.getElementById('login-page-zip').files[0];
     const n = (document.getElementById('login-page-name').value || '').trim();
     if (!f || !n) return;
     const r = await fetch('/console/custom-pages/upload?name=' + encodeURIComponent(n),
                           {method: 'POST', headers: {'Content-Type': 'application/zip'}, body: f})
                     .catch(() => null);
     location.href = r && r.redirected ? r.url : '/console/custom-pages?error=upload-failed';
   });")

(def login-json-codes
  [["credentials" "Wrong username or password — stay on the page and let them retry."]
   ["expired_code" "The login took too long. Start again from the app."]
   ["device_code_expired" "The device code expired. Start again on the device."]
   ["already_authorized" "This device code was already used."]
   ["ip_address" "IP address changed mid-flow (possible hijack). Start again."]
   ["user_agent" "Browser changed mid-flow (possible hijack). Start again."]
   ["challenge" "Device confirmation did not match (possible hijack). Start again."]
   ["broken_flow" "No valid state — the page was opened without one (direct visit or preview)."]
   ["unknown" "The flow in state is not one the server knows."]])

(def status-flows
  "login, authorization_code, device_code, federated, federated_link, onboarding, onboarding_claim")

(def index-snippet
  "<form id=\"login\" method=\"post\" action=\"/oauth/login\">
  <input type=\"hidden\" name=\"state\" id=\"state\">
  <input name=\"username\" autocomplete=\"username\">
  <input name=\"password\" type=\"password\" autocomplete=\"current-password\">
  <button>Sign in</button>
  <p id=\"error\"></p>
</form>
<script>
  const q = new URLSearchParams(location.search);
  document.getElementById('state').value = q.get('state') || '';
  document.getElementById('login').addEventListener('submit', async (e) => {
    e.preventDefault();
    const r = await fetch('/oauth/login', {
      method: 'POST', credentials: 'include',
      headers: {'Accept': 'application/json'},
      body: new URLSearchParams(new FormData(e.target))});
    const d = await r.json();
    if (d.ok && d.redirect) return location = d.redirect;
    if (d.ok && d.form_post) { /* build a form from d.form_post.params, submit to d.form_post.url */ }
    document.getElementById('error').textContent = MESSAGES[d.error] || d.error;
  });
</script>")

(def device-snippet
  "<!-- entry step: no ?user_code= -->
<form method=\"post\" action=\"/oauth/device/activate\">
  <input name=\"user_code\" placeholder=\"XXXX-XXXX\">
  <button>Continue</button>
</form>

<!-- confirm step: ?user_code=XXXX-XXXX -->
<form method=\"post\" action=\"/oauth/device/activate\">
  <p>Sign in on the device showing <b id=\"code\"></b>?</p>
  <button name=\"action\" value=\"confirm\">Confirm</button>
  <button name=\"action\" value=\"cancel\">Cancel</button>
</form>")

(def status-snippet
  "const q = new URLSearchParams(location.search);
const value = q.get('value');        // success | error | canceled
const error = q.get('error');        // a code from the table, or idp_<code>
const text = error
  ? (MESSAGES[error] || q.get('error_description') || 'Sign-in error: ' + error)
  : value === 'success'
    ? (q.get('client_name') ? q.get('client_name') + ' is authorized by ' + q.get('user') : 'Done.')
    : 'You canceled.';
document.getElementById('message').textContent = text;   // textContent, never innerHTML")

(def error-snippet
  "const q = new URLSearchParams(location.search);
const error = q.get('error');
const secure = q.get('secure') === 'true';
document.getElementById('message').textContent = MESSAGES[error] || 'Authorization error.';
if (secure) {
  // possibly an attack: warn not to enter credentials, offer no sign-in link
}")

(def claim-snippet
  "<form id=\"claim\" method=\"post\" action=\"/oauth/claim/password\">
  <input type=\"hidden\" name=\"token\" id=\"token\">
  <input name=\"password\" type=\"password\" autocomplete=\"new-password\">
  <button>Set password</button>
  <p id=\"error\"></p>
</form>
<div id=\"providers\"></div>
<script>
  const q = new URLSearchParams(location.search);
  const token = q.get('token') || '';
  const methods = q.get('methods') ? q.get('methods').split(',') : null;   // null = all
  document.getElementById('token').value = token;
  if (methods && !methods.includes('password')) document.getElementById('claim').hidden = true;
  if (q.get('error')) document.getElementById('error').textContent = MESSAGES[q.get('error')] || q.get('error');
  fetch('/oauth/federated/providers').then(r => r.json()).then(ps => ps
    .filter(p => !methods || methods.includes(p.name))
    .forEach(p => {
      const a = document.createElement('a');
      a.href = '/oauth/federated/start?' + new URLSearchParams({provider: p.name, mode: 'claim', state: token});
      a.textContent = 'Continue with ' + p.label;
      document.getElementById('providers').append(a);
    }));
</script>")

(def guides
  [{:file "index.html"
    :title "Sign-in page"
    :url "/login/<name>/?state=…"
    :summary (str "Every bundle needs it. The server sends the browser here with an opaque state; "
                  "the page collects username and password and posts them back together with that state.")
    :sections
    [{:heading "Receives (query string)"
      :cols ["Parameter" "Meaning"]
      :rows [["state" "Opaque, encrypted. Put it in a hidden field and send it back unchanged. Never parse it."]
             ["error" "Only after a failed Continue with X sign-in. Codes below."]]}
     {:heading "Must POST to /oauth/login"
      :text (str "Form-encoded (application/x-www-form-urlencoded). Use an explicit action — this page lives "
                 "under /login/, which is GET-only. Send Accept: application/json to get JSON back instead "
                 "of redirects, so errors stay on your page. Send credentials: 'include' so the session cookie sticks.")
      :cols ["Field" "Required" "Meaning"]
      :rows [["state" "yes" "The value from ?state=, unchanged."]
             ["username" "yes" "As typed."]
             ["password" "yes" "As typed."]]}
     {:heading "JSON answer"
      :cols ["Answer" "What the page does"]
      :rows [["{ok: true, redirect}" "location = redirect"]
             ["{ok: true, form_post: {url, params}}" "Create a form with one hidden input per params entry, action = url, method = post, submit it."]
             ["{ok: false, error}" "Show the message for error and let them retry. Codes below."]]}
     {:heading "JSON error codes"
      :cols ["Code" "Meaning"]
      :rows login-json-codes}
     {:heading "Continue with X (optional)"
      :text (str "GET /oauth/federated/providers returns the active providers as JSON. Render one link per provider "
                 "to /oauth/federated/start?provider=<name>&state=<state> — the same state. A failed attempt "
                 "comes back to this page with ?error=.")}
     {:heading "?error= codes"
      :cols ["Code" "Built-in text"]
      :rows (concat (sort login-page/federated-errors)
                    [["idp_<code>" "The provider's own OAuth error, e.g. idp_access_denied when the person cancels at the provider."]])}
     {:heading "Minimal page"
      :code index-snippet}]}

   {:file "device.html"
    :title "Device login"
    :url "/login/default/device.html"
    :summary (str "Default bundle only. For TVs and CLIs: the person types the code shown on the device, "
                  "then confirms it. Two steps, told apart by whether ?user_code= is present.")
    :sections
    [{:heading "Receives (query string)"
      :cols ["Parameter" "Meaning"]
      :rows [["user_code" "Absent on the entry step. On the confirm step, the code being confirmed — show it."]]}
     {:heading "Entry step: POST to /oauth/device/activate"
      :text "A valid code continues to sign-in. A wrong or expired one shows the built-in device page with the error."
      :cols ["Field" "Required" "Meaning"]
      :rows [["user_code" "yes" "The code as typed, e.g. ABCD-EFGH."]]}
     {:heading "Confirm step: POST to /oauth/device/activate"
      :text (str "Send nothing else — the browser carries a short-lived confirmation cookie the server set when it "
                 "redirected here. Use an explicit form action; this page lives under /login/.")
      :cols ["Field" "Required" "Meaning"]
      :rows [["action" "yes" "confirm or cancel."]]}
     {:heading "Minimal page"
      :code device-snippet}]}

   {:file "claim.html"
    :title "Account set-up"
    :url "/login/default/claim.html?token=…"
    :summary (str "Default bundle only. Where a sign-in link from Users → Generate sign-in link (or a provisioning "
                  "app's /oauth/onboard) lands: the invited person sets their first password or connects a provider. "
                  "Nobody else ever sets it.")
    :sections
    [{:heading "Receives (query string)"
      :cols ["Parameter" "Meaning"]
      :rows [["token" "The one-time link token. Send it back unchanged. Never parse it."]
             ["methods" "Comma list of allowed methods (password, provider names). Absent = all of them."]
             ["error" "Only after a rejected password. Codes below."]]}
     {:heading "Password: POST to /oauth/claim/password"
      :text (str "Form-encoded, plain form submit (no JSON mode). Success redirects to where the link was made for "
                 "(the console's sign-in for console links), else to the status page with flow=onboarding_claim. "
                 "A used or expired link goes to the status page with error=claim_invalid.")
      :cols ["Field" "Required" "Meaning"]
      :rows [["token" "yes" "The value from ?token=, unchanged."]
             ["password" "yes" "The new password. Confirm it on the page; the server does not."]]}
     {:heading "Continue with X"
      :text (str "GET /oauth/federated/providers lists the active providers. Link each allowed one to "
                 "/oauth/federated/start?provider=<name>&mode=claim&state=<token>.")}
     {:heading "?error= codes"
      :cols ["Code" "Built-in text"]
      :rows (sort onboarding/claim-errors)}
     {:heading "Minimal page"
      :code claim-snippet}]}

   {:file "status.html"
    :title "Status page"
    :url "/login/default/status.html?value=…"
    :summary (str "Default bundle only. Where a flow ends when there is no app to send the browser back to. "
                  "Read-only: display the outcome, POST nothing.")
    :sections
    [{:heading "Receives (query string)"
      :cols ["Parameter" "Meaning"]
      :rows [["value" "success, error or canceled. A cancelled device login is value=canceled; a cancelled browser login is value=error&error=cancelled."]
             ["error" "Code when value=error. Table below."]
             ["error_description" "Extra human text some errors carry."]
             ["flow" status-flows]
             ["user" "Who signed in (device success)."]
             ["client_name" "The app a device login was for. client carries its internal id."]
             ["provider" "Federated provider, e.g. after linking an account (value=success)."]]}
     {:heading "error codes"
      :cols ["Code" "Kind" "Built-in text"]
      :rows (concat (for [[code [kind text]] (sort status-page/status-errors)]
                      [code (if (= :retry kind) "can retry" "needs admin") text])
                    [["idp_<code>" "can retry" "The identity provider's own error, e.g. idp_access_denied."]])}
     {:heading "Minimal script"
      :code status-snippet}]}

   {:file "error.html"
    :title "Error page"
    :url "/login/default/error.html?error=…&secure=…"
    :summary (str "Default bundle only. An authorization request was rejected before sign-in, and the server must not "
                  "send the browser back to the app. Read-only: display the error, POST nothing.")
    :sections
    [{:heading "Receives (query string)"
      :cols ["Parameter" "Meaning"]
      :rows [["error" "Code. Table below."]
             ["secure" "true when the request may be an attack: tell them not to enter credentials and show no sign-in link."]
             ["client-name" "Only with invalid_login_page: the app."]
             ["invalid-value" "Only with invalid_login_page: its bad Login page setting."]]}
     {:heading "error codes"
      :cols ["Code" "secure" "Built-in title — text"]
      :rows (for [[code {:keys [title message]}] (sort error-page/error-messages)]
              [code (if (error-page/security-errors code) "true" "false")
               (str title " — " (or message "This app's Login page setting is not a bundle name."))])}
     {:heading "Minimal script"
      :code error-snippet}]}])

(defn guide-signal [file] (str "$pageGuide === '" file "'"))

(defn guide-section [{:keys [heading text cols rows code]}]
  [:div.console-guide-section
   [:div.console-ref-heading heading]
   (when text [:p text])
   (when rows
     [:table.console-table.console-ref-table
      [:thead [:tr (for [c cols] [:th c])]]
      [:tbody (for [row rows]
                [:tr (map-indexed (fn [i v] [:td (when (zero? i) {:class "mono"}) v]) row)])]])
   (when code [:pre.console-guide-code [:code code]])])

(defn guide-modal [{:keys [file title url summary sections]}]
  [:ty-modal {(keyword "data-attr:open") (guide-signal file)
              :backdrop "true" :close-on-outside-click true :close-on-escape true
              (keyword "data-on:close") "$pageGuide = ''"}
   [:div.console-confirm.console-guide-modal
    [:div.console-guide-head
     [:h2 [:code file] " — " title]
     [:p.mono url]]
    [:div.console-guide-body
     [:p summary]
     (map guide-section sections)]
    [:div.console-confirm-actions
     [:ty-button {:type "button" :size "sm" :appearance "ghost"
                  (keyword "data-on:click") "$pageGuide = ''"}
      "Close"]]]])

(def page-files
  [["index.html"  "Sign-in. Required in every bundle."]
   ["device.html" "Device code entry and confirm (TVs, CLIs). Default bundle only."]
   ["claim.html"  "Account set-up from a sign-in link — first password or provider. Default bundle only."]
   ["status.html" "Outcome after a login or device flow — success, cancel, error. Default bundle only."]
   ["error.html"  "Authorization request rejected before sign-in, e.g. a bad redirect URI. Default bundle only."]])

(defn upload-panel []
  [:div.console-panel.console-upload
   [:div.console-form-body
    [:div.console-settings-head (icon/icon :inbox {:size "13"}) "Upload"]
    [:div.console-upload-fields
     (ui/field-shell "Name" :text nil
                     [:ty-input {:id "login-page-name" :size "sm" :value custom/default-name
                                 :autocomplete "off"}])
     (ui/field-shell "Bundle" :text nil
                     [:ty-file-upload {:id "login-page-zip" :accept ".zip"
                                       :placeholder "Drop a zip or click to browse"}])
     [:ty-button {:id "login-page-upload" :type "button" :size "sm" :flavor "primary" :wide true}
      (icon/icon :upload {:size "12" :slot "start"}) "Upload"]]
    [:ul.console-upload-notes
     [:li "a zipped folder works too — up to 8 MB of html, css, js, json, images, fonts"]
     [:li "stored inactive — preview, then activate"]
     [:li [:code custom/default-name] " serves every client; any other name is picked per app"]]
    [:div.console-settings-head.console-upload-pages-head (icon/icon :layers {:size "13"}) "Pages"]
    [:dl.console-upload-pages
     (for [[file when-shown] page-files]
       (list [:dt [:a.console-guide-link {:href "#"
                                          (keyword "data-on:click")
                                          (str "evt.preventDefault(); $pageGuide = '" file "'")}
                   file]]
             [:dd when-shown]))]
    [:p.console-field-hint "Open a file for what it receives, what it must POST, and every code. "
     "Any file you leave out falls back to the built-in page."]]
   [:script (raw upload-script)]])

(defn render
  [request & [notice]]
  (ui/admin-shell
   {:title "Custom pages"
    :user (:console/principal request)
    :uri (:uri request)
    :confirm (str base "/activate")
    :session (:console/session request)
    :search :none
    :body
    [:div.console-page.console-page-scroll {:data-signals "{pageGuide: ''}"}
     [:div.console-page-head
      [:div.console-eyebrow "System"]
      [:h1.console-title "Custom pages"]
      [:p.console-subtitle
       (str "Your own sign-in, account set-up and status screens, served by this deployment at /login/<name>/. "
            "The console's own sign-in never uses them, so a broken bundle can always "
            "be switched off from here.")]]
     (widgets/notice notice true)
     [:div.console-login-pages
      (upload-panel)
      (bundles-table (custom/list-pages))]
     (map guide-modal guides)]}))
