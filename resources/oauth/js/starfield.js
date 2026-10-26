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

// Synthigy sky: the tooling starfield (seeded stars, nebulas, stardust, black hole) in three parallax layers.
(function boot() {
  if (!document.body) {
    document.addEventListener('DOMContentLoaded', boot);
    return;
  }
  if (document.getElementById('sy-starfield')) return;

  var SEED = 1969;
  var MARGIN = 16;
  var DIM = 0.72;
  var GLOW = 0.22;
  var MAIN = { count: 300, big: 0.94, durMin: 5, durMax: 11, delayMax: 9 };
  var NEBULAS = [
    { cx: 72, cy: 26, angle: -50, len: 9.5, wid: 5, count: 300, seedOffset: 1 },
    { cx: 38, cy: 72, angle: 24, len: 8, wid: 4.5, count: 240, seedOffset: 3 }
  ];
  var DUST = { count: 300, oMin: 0.08, oMax: 0.22, seedOffset: 2 };
  var FINE = { area: 2600, seedOffset: 5 };
  var TWINKLE = { share: 0.1, seedOffset: 11 };
  var HOLE = { x: 0.809, y: 0.382, vmin: 0.32, min: 200, max: 380, reach: 0.3, gap: 16, card: 440, ring: 0.183 };
  var DEPTH = { far: 2, mid: 5, near: 10 };
  var EASE = 0.06;

  var rgb = (getComputedStyle(document.documentElement).getPropertyValue('--sy-star-rgb') || '').trim() ||
    '255,255,255';

  function rgba(a) { return 'rgba(' + rgb + ',' + a + ')'; }

  function mulberry32(seed) {
    var a = seed;
    return function () {
      a = (a + 0x6D2B79F5) | 0;
      var t = Math.imul(a ^ (a >>> 15), a | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function gaussian(rnd) {
    return Math.sqrt(-2 * Math.log(Math.max(1e-9, rnd()))) * Math.cos(2 * Math.PI * rnd());
  }

  function media(q) {
    return window.matchMedia ? window.matchMedia(q) : { matches: false };
  }

  // ------------------------------------------------------------------ sky

  var near = [];
  var midTwinkle = [];
  var mid = [];
  var dust = [];
  var fine = [];

  (function () {
    var rnd = mulberry32(SEED);
    var pick = mulberry32(SEED + TWINKLE.seedOffset);
    for (var i = 0; i < MAIN.count; i++) {
      var x = rnd();
      var y = rnd();
      var big = rnd() > MAIN.big;
      var size = big ? 2 + rnd() * 1.6 : 0.6 + rnd() * 1.4;
      var base = 0.28 + rnd() * 0.62;
      var dur = MAIN.durMin + rnd() * (MAIN.durMax - MAIN.durMin);
      var delay = rnd() * MAIN.delayMax;
      var still = 0.35 + rnd() * 0.5;
      if (big) {
        near.push({ x: x, y: y, d: size, a: base * DIM, depth: 'near', glow: true,
                    per: dur, ph: delay, lo: 0.3, hi: 0.95, rest: 0.7 });
      } else if (pick() < TWINKLE.share) {
        var per = 4 + pick() * 5;
        midTwinkle.push({ x: x, y: y, d: size, a: base * still * DIM, depth: 'mid',
                          per: per, ph: pick() * per, lo: 0.55, hi: 1, rest: 1 });
      } else {
        mid.push({ x: x, y: y, d: size, a: base * still * DIM });
      }
    }

    for (var n = 0; n < NEBULAS.length; n++) {
      var neb = NEBULAS[n];
      var r = mulberry32(SEED + neb.seedOffset);
      var rad = neb.angle * Math.PI / 180;
      var cosa = Math.cos(rad);
      var sina = Math.sin(rad);
      for (var j = 0; j < neb.count; j++) {
        var along = neb.len * gaussian(r);
        var across = neb.wid * gaussian(r);
        var sz = 0.3 + r() * 0.9;
        var ba = 0.18 + r() * 0.45;
        mid.push({ x: (neb.cx + along * cosa - across * sina) / 100,
                   y: (neb.cy + along * sina + across * cosa) / 100,
                   d: sz, a: ba * DIM });
      }
    }

    var dr = mulberry32(SEED + DUST.seedOffset);
    for (var k = 0; k < DUST.count; k++) {
      var dx = dr();
      var dy = dr();
      var soft = dr() < 0.4;
      var o = DUST.oMin + dr() * (DUST.oMax - DUST.oMin);
      dust.push({ x: dx, y: dy, d: 1, a: o * DIM, soft: soft });
    }
  })();

  var twinkle = near.concat(midTwinkle);

  function fineStars(count) {
    var r = mulberry32(SEED + FINE.seedOffset);
    var out = [];
    for (var i = 0; i < count; i++) {
      var x = r();
      var y = r();
      var d = 0.6 + r() * 0.5;
      var a = 0.14 + Math.pow(r(), 2) * 0.34;
      out.push({ x: x, y: y, d: d, a: a });
    }
    return out;
  }

  // ------------------------------------------------------------------ DOM

  function el(tag, css, parent) {
    var e = document.createElement(tag);
    e.style.cssText = css;
    if (parent) parent.appendChild(e);
    return e;
  }

  var FILL = 'position:absolute;top:0;right:0;bottom:0;left:0;';
  var BOX = 'position:absolute;top:-' + MARGIN + 'px;right:-' + MARGIN + 'px;bottom:-' + MARGIN + 'px;left:-' +
    MARGIN + 'px;';
  var CANVAS = 'position:absolute;top:0;left:0;display:block;';

  var root = el('div', 'position:fixed;top:0;right:0;bottom:0;left:0;z-index:-1;pointer-events:none;' +
    'overflow:hidden;contain:strict;');
  root.id = 'sy-starfield';
  root.setAttribute('aria-hidden', 'true');

  var farLayer = el('div', BOX, root);
  var farCanvas = el('canvas', CANVAS, farLayer);
  var midLayer = el('div', BOX, root);

  var NOISE = 'url("data:image/svg+xml;utf8,' +
    "<svg xmlns='http://www.w3.org/2000/svg' width='900' height='500'>" +
    "<filter id='n'>" +
    "<feTurbulence type='fractalNoise' baseFrequency='0.005 0.011' numOctaves='3' seed='7' stitchTiles='stitch'/>" +
    "<feColorMatrix type='matrix' values='0 0 0 0 1 0 0 0 0 1 0 0 0 0 1 0 0 0 1.9 -0.5'/>" +
    "<feGaussianBlur stdDeviation='5'/>" +
    '</filter>' +
    "<rect width='100%25' height='100%25' filter='url(%23n)'/>" +
    '</svg>")';

  function blob(w, h, x, y, a) {
    return 'radial-gradient(' + w + '% ' + h + '% at ' + x + '% ' + y + '%, ' + rgba(a) + ', transparent 70%)';
  }

  var GAS = [blob(38, 45, 32, 44, 0.10), blob(28, 38, 55, 34, 0.08), blob(45, 55, 62, 60, 0.07),
             blob(22, 30, 42, 64, 0.09), blob(16, 22, 70, 42, 0.08), blob(60, 70, 48, 48, 0.05)].join(', ');

  for (var g = 0; g < NEBULAS.length; g++) {
    var nb = NEBULAS[g];
    var gas = el('div', 'position:absolute;left:' + nb.cx + '%;top:' + nb.cy + '%;width:' + nb.len * 5.5 +
      '%;height:' + nb.wid * 9 + '%;transform:translate(-50%,-50%) rotate(' + nb.angle + 'deg);opacity:' +
      (0.76 * DIM).toFixed(3) + ';', midLayer);
    el('div', 'width:100%;height:100%;background:' + GAS + ';-webkit-mask-image:' + NOISE + ';mask-image:' +
      NOISE + ';-webkit-mask-size:cover;mask-size:cover;', gas);
  }

  var midCanvas = el('canvas', CANVAS, midLayer);
  var twCanvas = el('canvas', 'position:absolute;top:-' + MARGIN + 'px;left:-' + MARGIN + 'px;display:block;', root);

  var holeLayer = el('div', BOX, root);
  var hole = el('div', 'position:absolute;display:none;', holeLayer);
  // Every hole gradient is closest-side and reaches transparent inside the box: no clipped edges.
  el('div', FILL + 'background:radial-gradient(circle closest-side, transparent 33%, ' + rgba(0.1) + ' 36.6%, ' +
    rgba(0.045) + ' 44%, ' + rgba(0.018) + ' 58%, ' + rgba(0.006) + ' 76%, transparent 100%);', hole);
  var BEAM = 'conic-gradient(from 0deg, rgba(0,0,0,.22), #000 70deg, #000 115deg, rgba(0,0,0,.22) 205deg, ' +
    'rgba(0,0,0,.1) 290deg, rgba(0,0,0,.22))';
  var arc = el('div', FILL + '-webkit-mask-image:' + BEAM + ';mask-image:' + BEAM + ';', hole);
  var RING = 'radial-gradient(circle closest-side, transparent 35%, ' + rgba(0.8) + ' 36.2%, ' + rgba(0.22) +
    ' 37.5%, transparent 40%)';
  el('div', FILL + 'background:' + RING + ';filter:blur(3px);opacity:.75;', arc);
  el('div', FILL + 'background:' + RING + ';', arc);
  el('div', FILL + 'background:radial-gradient(circle closest-side, #000 34.6%, transparent 35.8%);', hole);

  document.body.insertBefore(root, document.body.firstChild);

  var glow = document.createElement('canvas');
  glow.width = glow.height = 64;
  (function () {
    var c = glow.getContext('2d');
    var grad = c.createRadialGradient(32, 32, 0, 32, 32, 32);
    for (var i = 0; i < 8; i++) {
      var t = i / 8;
      grad.addColorStop(t, rgba(Math.exp(-4.5 * t * t).toFixed(3)));
    }
    grad.addColorStop(1, rgba(0));
    c.fillStyle = grad;
    c.fillRect(0, 0, 64, 64);
  })();

  // --------------------------------------------------------------- layout

  var vw, vh, W, H, dpr, lens, hx, hy, re;
  var midCtx, farCtx, twCtx;

  // Sub-2.5px stars snap to whole device pixels with alpha scaled by area: crisp points, same energy.
  function dot(ctx, x, y, d, a, ox, oy) {
    var D = d * dpr;
    if (D < 2.5) {
      var w = D < 1.5 ? 1 : 2;
      ctx.globalAlpha = Math.min(1, a * D * D / (w * w));
      ctx.fillRect(Math.round(x * dpr - w / 2) + ox, Math.round(y * dpr - w / 2) + oy, w, w);
    } else {
      ctx.globalAlpha = Math.min(1, a);
      ctx.beginPath();
      ctx.arc(x * dpr + ox, y * dpr + oy, D / 2, 0, 6.2832);
      ctx.fill();
    }
  }

  function blot(ctx, x, y, r, a, ox, oy) {
    var R = r * dpr;
    ctx.globalAlpha = Math.min(1, a);
    ctx.drawImage(glow, x * dpr - R + ox, y * dpr - R + oy, R * 2, R * 2);
  }

  // Lens: r' = r + Re^2/r, minimum 2Re at the photon ring; stars crowding the ring are brightened.
  function place(list) {
    for (var i = 0; i < list.length; i++) {
      var s = list[i];
      var x = s.x * W;
      var y = s.y * H;
      var b = 1;
      if (lens) {
        var dx = x - hx;
        var dy = y - hy;
        var r = Math.sqrt(dx * dx + dy * dy);
        if (r < 0.5) { dx = 0.5; dy = 0; r = 0.5; }
        var r2 = r + re * re / r;
        var q = (r2 - 2 * re) / (0.35 * re);
        x = hx + dx * r2 / r;
        y = hy + dy * r2 / r;
        b = 1 + 0.8 * Math.exp(-q * q);
      }
      s.px = x;
      s.py = y;
      s.b = b;
    }
  }

  function paint(ctx, list) {
    for (var i = 0; i < list.length; i++) {
      var s = list[i];
      var a = s.a * s.b;
      if (s.soft) blot(ctx, s.px, s.py, 2, a * 0.4, 0, 0);
      else dot(ctx, s.px, s.py, s.d, a, 0, 0);
    }
  }

  function fit(canvas, on) {
    canvas.width = on ? Math.round(W * dpr) : 0;
    canvas.height = on ? Math.round(H * dpr) : 0;
    canvas.style.width = W + 'px';
    canvas.style.height = H + 'px';
    var ctx = canvas.getContext('2d');
    ctx.fillStyle = 'rgb(' + rgb + ')';
    return ctx;
  }

  function layout() {
    vw = root.clientWidth || window.innerWidth;
    vh = root.clientHeight || window.innerHeight;
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = vw + 2 * MARGIN;
    H = vh + 2 * MARGIN;

    var card = document.querySelector('.sy-card');
    var edge = card ? card.getBoundingClientRect().right : (vw + HOLE.card) / 2;
    var s = Math.min(Math.max(HOLE.min, Math.min(HOLE.max, HOLE.vmin * Math.min(vw, vh))),
                     (HOLE.x * vw - edge - HOLE.gap) / HOLE.reach);
    lens = s >= HOLE.min;
    if (lens) {
      hx = MARGIN + HOLE.x * vw;
      hy = MARGIN + HOLE.y * vh;
      re = HOLE.ring * s / 2;
      hole.style.left = (hx - s / 2) + 'px';
      hole.style.top = (hy - s / 2) + 'px';
      hole.style.width = hole.style.height = s + 'px';
      hole.style.display = 'block';
    } else {
      hole.style.display = 'none';
    }

    fine = fineStars(Math.round(W * H / FINE.area));
    place(dust);
    place(fine);
    place(mid);
    place(twinkle);

    midCtx = fit(midCanvas, true);
    farCtx = fit(farCanvas, parallax);
    farLayer.style.display = parallax ? 'block' : 'none';
    var farTarget = parallax ? farCtx : midCtx;
    paint(farTarget, dust);
    paint(farTarget, fine);
    paint(midCtx, mid);
    farCtx.globalAlpha = midCtx.globalAlpha = 1;

    twCtx = fit(twCanvas, true);
    shiftLayers();
    drawTwinkle(window.performance ? performance.now() : Date.now(), !animating);
    start();
  }

  // -------------------------------------------------------------- motion

  var animating = false;
  var parallax = false;
  var raf = 0;
  var last = 0;
  var lastTwinkle = -1e9;
  var tx = 0, ty = 0, px = 0, py = 0;
  var settled = true;
  var shift = { far: [0, 0], mid: [0, 0], near: [0, 0] };
  var spin = null;

  function translate(s) { return 'translate3d(' + s[0] + 'px,' + s[1] + 'px,0)'; }

  // Settled offsets snap to device pixels so resting stars stay crisp on their composited layers.
  function shiftLayers() {
    for (var k in DEPTH) {
      var x = -px * DEPTH[k];
      var y = -py * DEPTH[k];
      if (settled) {
        x = Math.round(x * dpr) / dpr;
        y = Math.round(y * dpr) / dpr;
      }
      shift[k][0] = x;
      shift[k][1] = y;
    }
    var m = parallax ? translate(shift.mid) : '';
    farLayer.style.transform = parallax ? translate(shift.far) : '';
    midLayer.style.transform = m;
    holeLayer.style.transform = m;
  }

  function drawTwinkle(now, still) {
    twCtx.clearRect(0, 0, twCanvas.width, twCanvas.height);
    var sec = now / 1000;
    for (var i = 0; i < twinkle.length; i++) {
      var s = twinkle[i];
      var o = still ? s.rest : s.lo + (s.hi - s.lo) * (0.5 - 0.5 * Math.cos(6.2832 * (sec - s.ph) / s.per));
      var sh = shift[s.depth];
      var ox = sh[0] * dpr;
      var oy = sh[1] * dpr;
      if (s.glow) blot(twCtx, s.px, s.py, s.d * 3.4, GLOW * o * DIM * s.b, ox, oy);
      dot(twCtx, s.px, s.py, s.d, s.a * o * s.b, ox, oy);
    }
    twCtx.globalAlpha = 1;
  }

  function frame(now) {
    raf = 0;
    if (document.hidden || !animating) return;
    var dt = last ? Math.max(0, Math.min(100, now - last)) : 16.7;
    last = now;
    var moved = false;
    if (parallax && (px !== tx || py !== ty)) {
      var k = 1 - Math.pow(1 - EASE, dt / 16.7);
      px += (tx - px) * k;
      py += (ty - py) * k;
      settled = Math.abs(tx - px) * DEPTH.near < 0.01 && Math.abs(ty - py) * DEPTH.near < 0.01;
      if (settled) { px = tx; py = ty; }
      shiftLayers();
      moved = true;
    }
    // Twinkle periods are 4-11s, so ~30fps is indistinguishable; full rate only while layers move.
    if (moved || now - lastTwinkle > 32) {
      drawTwinkle(now, false);
      lastTwinkle = now;
    }
    raf = requestAnimationFrame(frame);
  }

  function start() {
    if (animating && !raf && !document.hidden) {
      last = 0;
      raf = requestAnimationFrame(frame);
    }
  }

  function spinArc(on) {
    if (on) {
      if (spin) spin.play();
      else if (arc.animate) {
        spin = arc.animate([{ transform: 'rotate(0deg)' }, { transform: 'rotate(360deg)' }],
                           { duration: 120000, iterations: Infinity });
      }
    } else if (spin) {
      spin.pause();
    }
  }

  var reduceMq = media('(prefers-reduced-motion: reduce)');
  var fineMq = media('(pointer: fine)');

  function mode() {
    animating = !reduceMq.matches;
    parallax = animating && fineMq.matches;
    if (!parallax) {
      tx = ty = px = py = 0;
      settled = true;
    }
    var hint = parallax ? 'transform' : 'auto';
    farLayer.style.willChange = midLayer.style.willChange = holeLayer.style.willChange = hint;
    spinArc(animating);
  }

  function onMode() {
    mode();
    if (!animating && raf) {
      cancelAnimationFrame(raf);
      raf = 0;
    }
    layout();
  }

  function watch(mq) {
    if (mq.addEventListener) mq.addEventListener('change', onMode);
    else if (mq.addListener) mq.addListener(onMode);
  }

  watch(reduceMq);
  watch(fineMq);

  window.addEventListener('pointermove', function (e) {
    if (!parallax || e.pointerType === 'touch') return;
    tx = Math.max(-1, Math.min(1, e.clientX / vw * 2 - 1));
    ty = Math.max(-1, Math.min(1, e.clientY / vh * 2 - 1));
  }, { passive: true });

  document.addEventListener('mouseout', function (e) {
    if (!e.relatedTarget) { tx = 0; ty = 0; }
  });

  document.addEventListener('visibilitychange', start);

  var pending = 0;
  window.addEventListener('resize', function () {
    if (!pending) {
      pending = requestAnimationFrame(function () {
        pending = 0;
        layout();
      });
    }
  });

  mode();
  layout();
})();
