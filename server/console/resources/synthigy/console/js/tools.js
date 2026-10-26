/*
 *   Synthigy — model-driven IAM and data platform
 *   Copyright (C) 2026 Robert Geršak
 *   SPDX-License-Identifier: BUSL-1.1
 *
 *   Licensed under the Business Source License 1.1 — see LICENSE. Free in
 *   production for individuals, non-profits, education and organisations
 *   up to EUR 5M; larger organisations, integrators deploying for them and
 *   hosted-service providers need a commercial license. Each version
 *   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
 *   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.
 */

// Opens <synthigy-tools> (Model · Data · Logs) from the console's own
// navigation. The bundle is served from this engine, not a CDN; see
// docs/core/synthigy/server/console.md.
(function () {
  "use strict";

  var BUNDLE = window.SYNTHIGY_TOOLING_BUNDLE || "/console/assets/js/tooling.js";

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

  var mounted = null;
  function mount() {
    if (!mounted) {
      mounted = document.createElement("synthigy-tools");
      mounted.setAttribute("endpoint", window.location.origin);
      mounted.addEventListener("close", function () { mounted.removeAttribute("open"); });
      document.body.appendChild(mounted);
    }
    return mounted;
  }

  function openTools() {
    loadBundle().then(function () {
      mount().setAttribute("open", "");
    }, function (err) {
      window.alert("Couldn't load Synthigy tooling: " + err.message);
    });
  }

  function wireNav() {
    var buttons = document.querySelectorAll("[data-syn-tools]");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener("click", function (e) {
        e.preventDefault();
        openTools();
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wireNav);
  } else {
    wireNav();
  }
})();
