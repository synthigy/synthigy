/*
 *   Synthigy — model-driven IAM and data platform
 *   Copyright (C) 2026 Robert Geršak
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as
 *   published by the Free Software Foundation, either version 3 of the
 *   License, or (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public
 *   License along with this program.  If not, see
 *   <https://www.gnu.org/licenses/>.
 *
 *   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
 *   in a proprietary product, or offering it as a service without
 *   releasing your source under section 13 — a commercial license is
 *   available: r.gersak@gmail.com  See COMMERCIAL.md.
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
