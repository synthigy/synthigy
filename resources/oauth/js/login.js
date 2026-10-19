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

// Synthigy Login

(function() {
  var userInput = document.getElementById('username');
  var passwordInput = document.getElementById('password');

  function setupRow(input) {
    if (!input) return;
    var row = input.closest('.row');
    if (!row) return;

    function updateActive() {
      if (input.value !== '' || document.activeElement === input) {
        row.classList.add('active');
      } else {
        row.classList.remove('active');
      }
    }

    input.addEventListener('focus', updateActive);
    input.addEventListener('blur', updateActive);
    input.addEventListener('input', updateActive);

    updateActive();
  }

  setupRow(userInput);
  setupRow(passwordInput);

  if (userInput && userInput.value && passwordInput) {
    passwordInput.focus();
  } else if (userInput) {
    userInput.focus();
  }
})();
