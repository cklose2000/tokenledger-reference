/* Repeatable, standalone reader adaptation of the retained Archify export. */
(() => {
  'use strict';
  const spec = JSON.parse(document.getElementById('tokenledger-reader-spec').textContent);
  const toolbar = document.querySelector('.toolbar');
  const toggle = document.createElement('button');
  toggle.id = 'reader-advanced'; toggle.type = 'button';
  toggle.textContent = 'Advanced tools'; toggle.setAttribute('aria-pressed', 'false');
  toggle.addEventListener('click', () => {
    const active = toggle.getAttribute('aria-pressed') !== 'true';
    document.documentElement.dataset.readerAdvanced = String(active);
    toggle.setAttribute('aria-pressed', String(active));
    toggle.textContent = active ? 'Simple view' : 'Advanced tools';
  });
  toolbar.append(toggle);
  const container = document.querySelector('.container');
  const intro = document.createElement('p'); intro.id = 'reader-intro';
  intro.innerHTML = '<a href="../assets/one-number.svg">Compact overview</a> · <a href="../demo.html">Watch the example</a> · <a href="../adoption.md">Adoption map</a>';
  const views = document.getElementById('guided-views');
  container.querySelector('.header').append(intro);
  const note = document.getElementById('guided-view-note');
  const noteVisibility = () => { note.hidden = note.textContent.startsWith('Step through curated'); };
  new MutationObserver(noteVisibility).observe(note, {childList: true, subtree: true});
  noteVisibility();
  const finder = document.getElementById('node-finder');
  // A fixed panel inside a transformed/contained diagram inherits its coordinate
  // system. Move this viewer-only panel to the document viewport, retaining the
  // original node and its event listeners. Never move authored graph elements.
  document.body.append(finder);
  const trigger = document.getElementById('btn-node-finder');
  const label = document.createElement('span'); label.textContent = 'Find'; trigger.append(label);
  const close = document.getElementById('node-finder-close');
  let returnFocus = trigger;
  trigger.addEventListener('click', () => { returnFocus = trigger; });
  finder.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      event.stopPropagation(); close.click(); returnFocus.focus();
    }
  });
  close.addEventListener('click', () => { returnFocus.focus(); });
  // Display authored business tags in finder badges; graph semantic types remain
  // unchanged for routing, validation, filtering and export.
  const nodes = Object.fromEntries(spec.components.map(n => [n.label, n]));
  const results = document.getElementById('node-finder-results');
  function domainBadges() {
    results.querySelectorAll('.node-finder-result').forEach(button => {
      const name = button.querySelector('strong')?.textContent;
      const badge = button.querySelector('em');
      const node = nodes[name];
      if (badge && node && badge.textContent !== node.tag) badge.textContent = node.tag;
      const detail = button.querySelector('small');
      const description = node && node.sublabel + ' · ' + node.id;
      if (detail && description && detail.textContent !== description) detail.textContent = description;
    });
  }
  new MutationObserver(domainBadges).observe(results, {childList: true, subtree: true});
  domainBadges();
})();
