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

// Behaviour shared by every OAuth page: host in the status line, busy submit,
// password reveal, Caps Lock tag, and messages written into the voice row.
(function () {
  var ICONS = {
    error: '<circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/>',
    warn: '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    success: '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>'
  };

  function svg(tone) {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75"' +
      ' stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
      (ICONS[tone] || ICONS.error) + '</svg>';
  }

  // Show a message in a voice row (it replaces the sub line); empty text restores the sub.
  function message(voice, text, tone) {
    if (typeof voice === 'string') voice = document.getElementById(voice);
    if (!voice) return;
    Array.prototype.forEach.call(voice.querySelectorAll('.sy-msg'), function (m) {
      voice.removeChild(m);
    });
    voice.classList.toggle('has-msg', !!text);
    if (!text) return;
    tone = tone || 'error';
    var box = document.createElement('div');
    box.className = 'sy-msg tone-' + tone;
    box.setAttribute('role', 'alert');
    var icon = document.createElement('span');
    icon.className = 'sy-msg-icon';
    icon.innerHTML = svg(tone);
    var label = document.createElement('span');
    label.className = 'sy-msg-text';
    label.title = text;
    label.textContent = text;
    box.appendChild(icon);
    box.appendChild(label);
    voice.appendChild(box);
  }

  function busy(form, on) {
    var btn = form.querySelector('button[type="submit"]:not([formnovalidate])');
    if (!btn) return;
    btn.classList.toggle('is-busy', on);
    if (on) btn.setAttribute('aria-busy', 'true');
    else btn.removeAttribute('aria-busy');
  }

  Array.prototype.forEach.call(document.querySelectorAll('[data-sy-host]'), function (el) {
    el.textContent = location.host;
  });

  // On document, so page validators bound to the form run first and can veto.
  document.addEventListener('submit', function (e) {
    var form = e.target;
    if (!form.hasAttribute || !form.hasAttribute('data-sy-busy') || e.defaultPrevented) return;
    if (form.dataset.syPending) { e.preventDefault(); return; }
    form.dataset.syPending = '1';
    busy(form, true);
  });

  // Back/forward cache restores the page mid-submit; give the form back.
  addEventListener('pageshow', function (e) {
    if (!e.persisted) return;
    Array.prototype.forEach.call(document.querySelectorAll('form[data-sy-busy]'), function (form) {
      delete form.dataset.syPending;
      busy(form, false);
    });
  });

  Array.prototype.forEach.call(document.querySelectorAll('[data-sy-reveal]'), function (btn) {
    var input = document.getElementById(btn.getAttribute('data-sy-reveal'));
    if (!input) return;
    btn.addEventListener('mousedown', function (e) { e.preventDefault(); });
    btn.addEventListener('click', function () {
      var show = input.type === 'password';
      input.type = show ? 'text' : 'password';
      btn.setAttribute('aria-pressed', show ? 'true' : 'false');
      btn.setAttribute('aria-label', show ? 'Hide password' : 'Show password');
    });
  });

  Array.prototype.forEach.call(document.querySelectorAll('[data-sy-caps]'), function (tag) {
    var input = document.getElementById(tag.getAttribute('data-sy-caps'));
    if (!input) return;
    function check(e) {
      if (e.getModifierState) tag.classList.toggle('on', e.getModifierState('CapsLock'));
    }
    input.addEventListener('keydown', check);
    input.addEventListener('keyup', check);
    input.addEventListener('blur', function () { tag.classList.remove('on'); });
  });

  Array.prototype.forEach.call(document.querySelectorAll('.sy-input[aria-invalid="true"]'), function (input) {
    input.addEventListener('input', function () {
      input.removeAttribute('aria-invalid');
    }, { once: true });
  });

  window.SynthigyPage = { message: message, busy: busy };
})();
