;(function () {
  'use strict'

  // Mobile nav menu toggle logic.
  // - Uses hidden + aria-expanded for accessibility.
  // - Closes on Escape and outside click.
  // - Auto-closes when resizing up to desktop breakpoint.

  // Return true when the viewport is within the mobile breakpoint.
  function isMobile() {
    return window.matchMedia && window.matchMedia('(max-width: 520px)').matches
  }

  // Open the mobile menu and update ARIA state.
  function openMenu(btn, menu) {
    menu.hidden = false
    btn.setAttribute('aria-expanded', 'true')
    btn.setAttribute('aria-label', 'Close menu')
  }

  // Close the mobile menu and update ARIA state.
  function closeMenu(btn, menu) {
    menu.hidden = true
    btn.setAttribute('aria-expanded', 'false')
    btn.setAttribute('aria-label', 'Open menu')
  }

  // Check whether the menu is currently open.
  function isOpen(btn, menu) {
    return btn.getAttribute('aria-expanded') === 'true' && menu.hidden === false
  }

  document.addEventListener('DOMContentLoaded', function () {
    var btn = document.getElementById('nav-menu-toggle')
    var menu = document.getElementById('nav-mobile-menu')
    if (!btn || !menu) return

    closeMenu(btn, menu)

    btn.addEventListener('click', function (e) {
      e.preventDefault()
      if (!isMobile()) return

      if (isOpen(btn, menu)) {
        closeMenu(btn, menu)
      } else {
        openMenu(btn, menu)
      }
    })

    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return
      if (isOpen(btn, menu)) closeMenu(btn, menu)
    })

    document.addEventListener('click', function (e) {
      if (!isOpen(btn, menu)) return
      var t = e.target
      if (btn.contains(t)) return
      if (menu.contains(t)) return
      closeMenu(btn, menu)
    })

    window.addEventListener('resize', function () {
      if (!isMobile() && isOpen(btn, menu)) closeMenu(btn, menu)
    })

    menu.addEventListener('click', function (e) {
      var t = e.target
      if (t && t.tagName === 'A') closeMenu(btn, menu)
    })
  })
})()
