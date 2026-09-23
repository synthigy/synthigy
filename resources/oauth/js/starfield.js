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

// Background starfield — canvas, per-star twinkle + soft nebula clouds.
// Reads --sy-star-rgb from the page so it themes with the CSS.
//
// Per-frame budget is deliberately tiny: the nebulas are CSS gradients on the
// canvas element (compositor paints them once), the glow halo is one
// pre-rendered sprite, and sub-pixel stars are fillRect'd rather than arc'd.
// Only the twinkle actually needs JS.
(function () {
  var canvas = document.createElement('canvas');
  canvas.id = 'sy-starfield';
  canvas.setAttribute('aria-hidden', 'true');
  document.body.insertBefore(canvas, document.body.firstChild);
  var ctx = canvas.getContext('2d');
  var reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;

  var W, H, DPR, stars, rgb, glow;

  function starColor() {
    var v = getComputedStyle(document.documentElement)
      .getPropertyValue('--sy-star-rgb').trim();
    return v || '255,255,255';
  }

  function rand(a, b) { return a + Math.random() * (b - a); }
  // standard normal (Box-Muller) — for gaussian star clusters
  function gauss() {
    return Math.sqrt(-2 * Math.log(1 - Math.random())) * Math.cos(6.283 * Math.random());
  }

  function star(x, y, sz) {
    return {
      x: x, y: y,
      r: (0.3 + sz * 1.1) * DPR,
      a: rand(0.2, 0.55) + sz * 0.4,   // bigger = brighter
      sp: rand(1.2, 3),                // twinkle speed
      ph: Math.random() * 6.283,       // phase
      gl: 0.3 + sz * 1.1 > 1.1         // big enough for a glow halo
    };
  }

  // The two nebula clouds don't move and don't twinkle — hand them to CSS and
  // they never touch a frame again. (They used to be two full-screen
  // createRadialGradient + fillRect per frame: ~66M pixel writes/frame at 4K.)
  function nebulas() {
    canvas.style.background =
      'radial-gradient(circle 42vmax at 74% 24%, rgba(' + rgb + ',.055), rgba(' + rgb + ',0)),' +
      'radial-gradient(circle 38vmax at 30% 76%, rgba(' + rgb + ',.045), rgba(' + rgb + ',0))';
  }

  // One glow sprite, drawn scaled per big star — beats allocating a fresh
  // gradient object per star per frame.
  function buildGlow() {
    var R = 32;
    glow = document.createElement('canvas');
    glow.width = glow.height = R * 2;
    var c = glow.getContext('2d');
    var g = c.createRadialGradient(R, R, 0, R, R, R);
    g.addColorStop(0, 'rgba(' + rgb + ',.5)');
    g.addColorStop(1, 'rgba(' + rgb + ',0)');
    c.fillStyle = g;
    c.fillRect(0, 0, R * 2, R * 2);
  }

  function layout() {
    DPR = Math.min(window.devicePixelRatio || 1, 2);
    W = canvas.width = Math.floor(innerWidth * DPR);
    H = canvas.height = Math.floor(innerHeight * DPR);
    canvas.style.width = innerWidth + 'px';
    canvas.style.height = innerHeight + 'px';
    nebulas();
  }

  function build() {
    rgb = starColor();
    layout();
    buildGlow();

    stars = [];

    // 1. uniform field — steep size skew → mostly tiny pinpricks, rare big
    var n = Math.round(innerWidth * innerHeight / 1200); // density (scales w/ window)
    for (var i = 0; i < n; i++) {
      stars.push(star(Math.random() * W, Math.random() * H, Math.pow(Math.random(), 5)));
    }

    // 2. groupings — gaussian clusters (open clusters / milky-way patches).
    //    Two anchored on the nebulae for cohesion, the rest scattered.
    var groups = [{ cx: 0.74, cy: 0.24 }, { cx: 0.30, cy: 0.76 }];
    var extra = 3 + Math.floor(Math.random() * 3);       // 3–5 more
    for (var g = 0; g < extra; g++) {
      groups.push({ cx: Math.random(), cy: Math.random() });
    }
    for (var j = 0; j < groups.length; j++) {
      var cx = groups[j].cx * W, cy = groups[j].cy * H;
      var sx = rand(0.03, 0.08) * W, sy = rand(0.03, 0.08) * H;  // elongated spread
      var m = 40 + Math.floor(Math.random() * 70);
      for (var k = 0; k < m; k++) {
        // cluster stars: tighter, fainter, mostly tiny
        stars.push(star(cx + gauss() * sx, cy + gauss() * sy, Math.pow(Math.random(), 5) * 0.7));
      }
    }
  }

  function frame(t) {
    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = 'rgb(' + rgb + ')';

    for (var i = 0; i < stars.length; i++) {
      var s = stars[i];
      var pulse = 0.5 + 0.5 * Math.sin(t * 0.0016 * s.sp + s.ph); // 0..1
      ctx.globalAlpha = s.a * (0.35 + 0.65 * pulse);
      if (s.gl) {
        var gr = s.r * 2.2;
        ctx.drawImage(glow, s.x - gr, s.y - gr, gr * 2, gr * 2);
      }
      if (s.r <= DPR) {
        // sub-pixel dot — a square and a circle are the same pixels here,
        // and fillRect skips the path machinery entirely
        ctx.fillRect(s.x - s.r, s.y - s.r, s.r * 2, s.r * 2);
      } else {
        ctx.beginPath();
        ctx.arc(s.x, s.y, s.r, 0, 6.283);
        ctx.fill();
      }
    }
    ctx.globalAlpha = 1;
    if (!reduce) requestAnimationFrame(frame);
  }

  build();
  requestAnimationFrame(frame);

  // Resize re-fits the canvas and shifts stars by half the size delta —
  // anchors the field to the viewport CENTER instead of the top-left
  // corner (which is where an unadorned canvas.width/height resize pins
  // it), so growth/shrink reads the same regardless of which edge of the
  // window you actually dragged.
  function resize() {
    var oldW = W, oldH = H;
    layout();
    var dx = (W - oldW) / 2, dy = (H - oldH) / 2;
    for (var i = 0; i < stars.length; i++) {
      stars[i].x += dx;
      stars[i].y += dy;
    }
  }
  addEventListener('resize', resize);
})();
