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

// Segmented device-code entry (PIN/OTP style).
// Drives the .code-box inputs: auto-advance, backspace, arrows, paste, and
// assembles the value into the hidden #user_code field as XXXX-XXXX.
// Read-only boxes (the confirm step) are skipped — nothing to drive there.
(function () {
  var boxes = Array.prototype.slice
    .call(document.querySelectorAll('.code-box'))
    .filter(function (b) { return !b.readOnly && !b.disabled; });
  if (!boxes.length) return;

  var hidden = document.getElementById('user_code');
  var ALLOWED = /[A-Z0-9]/; // server validates the exact charset

  function assemble() {
    if (!hidden) return;
    var v = boxes.map(function (b) { return b.value; }).join('');
    hidden.value = v.length > 4 ? v.slice(0, 4) + '-' + v.slice(4) : v;
  }
  function focusBox(i) { if (boxes[i]) { boxes[i].focus(); boxes[i].select(); } }

  boxes.forEach(function (box, i) {
    box.addEventListener('input', function () {
      var m = box.value.toUpperCase().match(ALLOWED);
      box.value = m ? m[0] : '';
      if (box.value && i < boxes.length - 1) focusBox(i + 1);
      assemble();
    });
    box.addEventListener('keydown', function (e) {
      if (e.key === 'Backspace' && !box.value && i > 0) focusBox(i - 1);
      else if (e.key === 'ArrowLeft' && i > 0) focusBox(i - 1);
      else if (e.key === 'ArrowRight' && i < boxes.length - 1) focusBox(i + 1);
    });
    box.addEventListener('paste', function (e) {
      e.preventDefault();
      var text = ((e.clipboardData && e.clipboardData.getData('text')) || '')
        .toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, boxes.length);
      for (var j = 0; j < text.length && (i + j) < boxes.length; j++) {
        boxes[i + j].value = text[j];
      }
      focusBox(Math.min(i + text.length, boxes.length - 1));
      assemble();
    });
  });

  focusBox(0);
})();
