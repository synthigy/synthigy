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

// Device code entry: segmented boxes composed into #user_code as XXXX-XXXX,
// and the busy state on whichever confirm-step action was pressed.
(function () {
  var form = document.getElementById('device-form');
  var hidden = document.getElementById('user_code');
  var boxes = Array.prototype.slice
    .call(document.querySelectorAll('.code-box'))
    .filter(function (b) { return !b.readOnly && !b.disabled; });

  function clean(text) {
    return (text || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  }

  function code() {
    return boxes.map(function (b) { return b.value; }).join('');
  }

  function assemble() {
    if (!hidden) return;
    var v = code();
    hidden.value = v.length > 4 ? v.slice(0, 4) + '-' + v.slice(4) : v;
  }

  function focusBox(i) {
    var b = boxes[i];
    if (b) { b.focus(); b.select(); }
  }

  function firstEmpty() {
    for (var i = 0; i < boxes.length; i++) if (!boxes[i].value) return i;
    return -1;
  }

  function valid() {
    boxes.forEach(function (b) { b.removeAttribute('aria-invalid'); });
  }

  function fill(i, text) {
    var start = text.length >= boxes.length ? 0 : i;
    for (var j = 0; j < text.length && start + j < boxes.length; j++) {
      boxes[start + j].value = text[j];
    }
    focusBox(Math.min(start + text.length, boxes.length - 1));
    assemble();
  }

  boxes.forEach(function (box, i) {
    box.addEventListener('input', function () {
      valid();
      var v = clean(box.value);
      if (v.length > 1) { fill(i, v.slice(0, boxes.length)); return; }
      box.value = v;
      if (v && i < boxes.length - 1) focusBox(i + 1);
      assemble();
    });
    box.addEventListener('keydown', function (e) {
      if (e.key === 'Backspace' && !box.value && i > 0) focusBox(i - 1);
      else if (e.key === 'ArrowLeft' && i > 0) focusBox(i - 1);
      else if (e.key === 'ArrowRight' && i < boxes.length - 1) focusBox(i + 1);
    });
    box.addEventListener('click', function () { box.select(); });
    box.addEventListener('paste', function (e) {
      e.preventDefault();
      var v = clean(e.clipboardData && e.clipboardData.getData('text')).slice(0, boxes.length);
      if (!v) return;
      valid();
      fill(i, v);
    });
  });

  if (boxes.length && form) {
    form.addEventListener('submit', function (e) {
      assemble();
      var gap = firstEmpty();
      if (gap < 0) return;
      e.preventDefault();
      boxes.forEach(function (b) { if (!b.value) b.setAttribute('aria-invalid', 'true'); });
      if (window.SynthigyPage) {
        window.SynthigyPage.message('voice', 'Enter all ' + boxes.length + ' characters of the code.');
      }
      focusBox(gap);
    });
    assemble();
    var gap = firstEmpty();
    focusBox(gap < 0 ? 0 : gap);
  }

  var actions = form ? form.querySelectorAll('button[name="action"]') : [];
  if (actions.length) {
    var pressed = null;
    Array.prototype.forEach.call(actions, function (btn) {
      btn.addEventListener('click', function () { pressed = btn; });
    });
    form.addEventListener('submit', function (e) {
      var btn = e.submitter || pressed;
      if (!btn || btn.name !== 'action') return;
      btn.classList.add('is-busy');
      btn.setAttribute('aria-busy', 'true');
    });
    addEventListener('pageshow', function (e) {
      if (!e.persisted) return;
      Array.prototype.forEach.call(actions, function (btn) {
        btn.classList.remove('is-busy');
        btn.removeAttribute('aria-busy');
      });
    });
  }
})();
