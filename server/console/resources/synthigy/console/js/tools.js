/*
 *   Synthigy — model-driven IAM and data platform
 *   Copyright (C) 2026 Robert Geršak
 *   SPDX-License-Identifier: Elastic-2.0
 *
 *   Licensed under the Elastic License 2.0 — see LICENSE, or
 *   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
 *   and build your own products on; you may not provide Synthigy to third
 *   parties as a hosted or managed service. Commercial licensing:
 *   r.gersak@gmail.com  See COMMERCIAL.md.
 */

// Opens the Synthigy web components (modeler, data console, log cockpit) from
// the console's own navigation. The bundle is served from this engine, not a
// CDN — see docs/core/synthigy/server/console.md.
//
// This one file runs on every console page AND on /console/tools/callback,
// which is the single redirect address registered for the Synthigy Tools
// client. Three shapes, in order:
//
//   1. loaded in an IFRAME carrying ?code&state — a silent renewal. Answer the
//      parent and stop. The bundle is never loaded here: the login only wants
//      a postMessage of this URL, and 2.2MB does not fit inside the renewal
//      timeout.
//   2. loaded as the TOP window on the callback path — a login coming back.
//      Bounce the query to the console page the operator left, which is where
//      the component lives and can settle it. Anything else here returns to
//      that page without the query.
//   3. any console page — wire the nav buttons, and remount the last-opened
//      tool so a returning login (case 2) lands on something that can finish.
(function () {
  "use strict";

  var BUNDLE = window.SYNTHIGY_TOOLING_BUNDLE || "/console/assets/js/tooling.js";
  var CALLBACK = "/console/tools/callback";
  var LAST_OPEN_KEY = "syn-tools-last-open";
  var RETURN_KEY = "syn-tools-return";
  var TAGS = {
    model: "synthigy-data-modeling",
    data: "synthigy-data-console",
    logs: "synthigy-log-cockpit"
  };

  function store(k, v) {
    try { window.sessionStorage.setItem(k, v); } catch (e) { /* private mode */ }
  }
  function read(k) {
    try { return window.sessionStorage.getItem(k); } catch (e) { return null; }
  }
  function looksLikeOAuthReturn() {
    var q = window.location.search;
    return /state=/.test(q) && (/code=/.test(q) || /error=/.test(q));
  }
  function onCallbackPage() {
    return window.location.pathname === CALLBACK;
  }

  var loading = null;
  function loadBundle() {
    if (loading) return loading;
    loading = new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = BUNDLE;
      s.onload = resolve;
      s.onerror = function () { loading = null; reject(new Error("tooling bundle failed to load")); };
      document.head.appendChild(s);
    });
    return loading;
  }

  var mounted = {};
  function mount(key) {
    if (mounted[key]) return mounted[key];
    var el = document.createElement(TAGS[key]);
    el.setAttribute("endpoint", window.location.origin);
    el.addEventListener("close", function () { el.removeAttribute("open"); });
    document.body.appendChild(el);
    mounted[key] = el;
    return el;
  }

  // show === false mounts without opening — enough for connectedCallback to run
  // the same auth restore a click would, with nothing appearing on screen.
  function openTool(key, show) {
    if (!TAGS[key]) return;
    store(LAST_OPEN_KEY, key);
    store(RETURN_KEY, window.location.pathname);
    loadBundle().then(function () {
      var el = mount(key);
      if (show !== false) el.setAttribute("open", "");
    }, function (err) {
      if (show !== false) window.alert("Couldn't load Synthigy tooling: " + err.message);
    });
  }

  // must match synthigy.client.login/silent-callback! — any other source is ignored and the renewal times out
  function answerSilentRenew() {
    if (window.self === window.top || !looksLikeOAuthReturn()) return false;
    window.parent.postMessage(
      { source: "synthigy-login", url: window.location.href },
      window.location.origin);
    return true;
  }

  // The code was delivered HERE because this path is what the client has
  // registered, but the component that must consume it lives on the console
  // page. The login reads code+state off the URL and does not require the
  // page to be the registered address, so carrying the query across is enough.
  function bounceBack() {
    var to = read(RETURN_KEY) || "/console/sessions";
    window.location.replace(to + window.location.search);
  }

  function wireNav() {
    var buttons = document.querySelectorAll("[data-syn-tool]");
    for (var i = 0; i < buttons.length; i++) {
      (function (btn) {
        btn.addEventListener("click", function (e) {
          e.preventDefault();
          openTool(btn.getAttribute("data-syn-tool"));
        });
      })(buttons[i]);
    }
  }

  function init() {
    if (answerSilentRenew()) return;
    if (onCallbackPage()) {
      if (looksLikeOAuthReturn()) bounceBack();
      else window.location.replace(read(RETURN_KEY) || "/console");
      return;
    }
    wireNav();
    var key = read(LAST_OPEN_KEY);
    if (key) openTool(key, looksLikeOAuthReturn());
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
