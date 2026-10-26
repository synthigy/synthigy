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

// Synthigy sign-in and account setup
(function () {
  var user = document.getElementById('username');
  var pass = document.getElementById('password');

  if (user && user.value && pass) pass.focus();
  else if (user) user.focus();

  var claim = document.getElementById('claim-password-form');
  if (claim) {
    var first = document.getElementById('claim-password');
    var confirm = document.getElementById('claim-password-confirm');
    first.focus();
    claim.addEventListener('submit', function (e) {
      if (!first.value) {
        e.preventDefault();
        window.SynthigyPage.message('claim-msg', 'Choose a password.');
        first.focus();
      } else if (first.value !== confirm.value) {
        e.preventDefault();
        confirm.setAttribute('aria-invalid', 'true');
        window.SynthigyPage.message('claim-msg', 'Passwords do not match.');
        confirm.focus();
      }
    });
    confirm.addEventListener('input', function () {
      confirm.removeAttribute('aria-invalid');
    });
  }
})();
