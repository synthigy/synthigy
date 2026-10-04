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
