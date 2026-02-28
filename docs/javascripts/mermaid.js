// This file enables Mermaid diagrams in MkDocs Material and re-renders them on
// client-side navigation (navigation.instant).
;(() => {
  // render finds all `.mermaid` blocks on the current page and (re)renders them.
  const render = () => {
    if (typeof mermaid === 'undefined') {
      return
    }

    const scheme = document.body.getAttribute('data-md-color-scheme')
    const theme = scheme === 'slate' ? 'dark' : 'default'

    mermaid.initialize({ startOnLoad: false, theme })

    const nodes = Array.from(document.querySelectorAll('.mermaid'))
    if (nodes.length === 0) {
      return
    }

    nodes.forEach((n) => {
      const existing = n.getAttribute('data-mermaid-source')
      if (!existing) {
        n.setAttribute('data-mermaid-source', n.textContent.trim())
      }

      n.textContent = n.getAttribute('data-mermaid-source') || ''
      n.removeAttribute('data-processed')
    })

    const p = mermaid.run({ nodes })
    if (p && typeof p.catch === 'function') {
      p.catch(() => {})
    }
  }

  if (typeof document$ !== 'undefined') {
    document$.subscribe(() => render())

    const obs = new MutationObserver(() => render())
    obs.observe(document.body, { attributes: true, attributeFilter: ['data-md-color-scheme'] })

    return
  }

  window.addEventListener('DOMContentLoaded', () => render())
})()
