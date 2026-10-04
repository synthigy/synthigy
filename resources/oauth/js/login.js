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

  if (userInput) {
    userInput.focus();
  }
})();
