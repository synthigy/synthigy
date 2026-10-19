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
