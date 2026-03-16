(function () {
  'use strict';

  var svg = document.getElementById('storage-svg');
  if (!svg) return;

  var layer = document.getElementById('packet-layer');

  // ── easing ────────────────────────────────────────────────────────────────

  function easeInOut(t) {
    return t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
  }

  // ── packet factory ────────────────────────────────────────────────────────

  function makePacket(color, radius) {
    var dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    dot.setAttribute('r', radius);
    dot.setAttribute('fill', color);
    dot.style.opacity = '0';
    return dot;
  }

  // ── animate a packet along an SVG path ────────────────────────────────────
  // Returns a Promise that resolves when the packet reaches the end.

  function animateAlongPath(pathEl, color, radius, durationMs) {
    return new Promise(function (resolve) {
      var len = pathEl.getTotalLength();
      var dot = makePacket(color, radius);
      layer.appendChild(dot);

      var start = null;

      function tick(ts) {
        if (!start) start = ts;
        var elapsed = ts - start;
        var t = elapsed / durationMs;
        if (t > 1) t = 1;

        var te = easeInOut(t);
        var pt = pathEl.getPointAtLength(te * len);
        dot.setAttribute('cx', pt.x);
        dot.setAttribute('cy', pt.y);

        // fade in for first 15%, fade out for last 15%
        var opacity;
        if (t < 0.15) {
          opacity = t / 0.15;
        } else if (t > 0.82) {
          opacity = 1 - (t - 0.82) / 0.18;
        } else {
          opacity = 1;
        }
        dot.style.opacity = Math.max(0, opacity).toFixed(3);

        if (t < 1) {
          requestAnimationFrame(tick);
        } else {
          if (dot.parentNode) layer.removeChild(dot);
          resolve();
        }
      }

      requestAnimationFrame(tick);
    });
  }

  // ── flash a rect element's stroke ─────────────────────────────────────────

  function flash(el, color, holdMs) {
    if (!el) return;
    var origStroke = el.getAttribute('stroke');
    var origWidth = el.getAttribute('stroke-width');
    el.setAttribute('stroke', color);
    el.setAttribute('stroke-width', '3');
    setTimeout(function () {
      el.setAttribute('stroke', origStroke);
      el.setAttribute('stroke-width', origWidth);
    }, holdMs || 220);
  }

  // ── path & disk configuration ─────────────────────────────────────────────

  var pfsBox   = document.getElementById('pfs-box');
  var mountBox = document.getElementById('mount-box');
  var pathOut  = document.getElementById('path-out');

  var diskPaths = [
    // SSDs: fast packets, short travel, vivid green
    { id: 'path-ssd1', color: '#48bb78', radius: 4,   travelMs: 750,  intervalMs: 1300 },
    { id: 'path-ssd2', color: '#48bb78', radius: 4,   travelMs: 850,  intervalMs: 1600 },
    // HDDs: slower packets, longer travel, steel blue-gray
    { id: 'path-hdd1', color: '#94a3b8', radius: 3.5, travelMs: 1400, intervalMs: 2100 },
    { id: 'path-hdd2', color: '#94a3b8', radius: 3.5, travelMs: 1550, intervalMs: 2500 },
    { id: 'path-hdd3', color: '#94a3b8', radius: 3.5, travelMs: 1650, intervalMs: 2900 },
  ];

  // ── fire a packet from one disk, then emit one out-packet after arrival ───

  function fireDiskPacket(cfg) {
    var pathEl = document.getElementById(cfg.id);
    if (!pathEl) return;

    animateAlongPath(pathEl, cfg.color, cfg.radius, cfg.travelMs).then(function () {
      flash(pfsBox, '#6b9df5', 200);

      // Brief delay, then emit unified out-packet
      setTimeout(function () {
        if (pathOut) {
          animateAlongPath(pathOut, '#3b6ff0', 4.5, 480).then(function () {
            flash(mountBox, '#3b6ff0', 200);
          });
        }
      }, 120);
    });
  }

  // ── schedule recurring packets for each disk ──────────────────────────────

  function scheduleDisk(cfg) {
    // Stagger initial fire so all five don't launch simultaneously
    var initialDelay = Math.random() * cfg.intervalMs;

    setTimeout(function fire() {
      fireDiskPacket(cfg);
      // jitter ±10% to avoid lockstep
      var jitter = (Math.random() * 0.2 - 0.1) * cfg.intervalMs;
      setTimeout(fire, cfg.intervalMs + jitter);
    }, initialDelay);
  }

  diskPaths.forEach(scheduleDisk);

})();
