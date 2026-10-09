/* Interaction shared by the three pages. No framework, no network.
 *
 * Everything here is an enhancement over markup that already works. The theme
 * toggle has a default, the tables are readable unsorted, and the chart
 * readouts show a value before anything is hovered. A page that needs its
 * JavaScript to be legible is a page that is illegible in a screenshot, which
 * is how most of these are actually seen.
 */
(function () {
  'use strict';

  /* ------------------------------------------------------------ theme */

  var root = document.documentElement;

  function stored() {
    try { return localStorage.getItem('theme'); } catch (e) { return null; }
  }

  function remember(value) {
    try { localStorage.setItem('theme', value); } catch (e) { /* private mode */ }
  }

  // Dark is the default because the architecture diagram is dark and the three
  // tabs have to agree. A stored choice wins over that.
  var saved = stored();
  if (saved === 'light' || saved === 'dark') {
    root.setAttribute('data-theme', saved);
  }

  function label(btn) {
    var light = root.getAttribute('data-theme') === 'light';
    btn.textContent = light ? 'Dark' : 'Light';
    btn.setAttribute('aria-label',
      'Switch to ' + (light ? 'dark' : 'light') + ' theme');
  }

  // A wrapper page frames a diagram in an iframe rather than owning its
  // markup (see the wrapper's own comment for why). The diagram keeps its own
  // theme under a different localStorage key, 'archify-theme', and reads it
  // only once, on its own load -- so without this, the site's Light/Dark
  // button flips the chrome around the diagram and leaves the diagram dark,
  // which looks like light mode half-worked rather than like two independent
  // toggles. Same origin, so both the stored key and a direct attribute write
  // reach an already-loaded iframe immediately.
  function syncDiagramTheme(value) {
    try { localStorage.setItem('archify-theme', value); } catch (e) { /* private mode */ }
    var frame = document.getElementById('diagramFrame');
    var doc = frame && frame.contentDocument;
    if (doc && doc.documentElement) {
      doc.documentElement.setAttribute('data-theme', value);
    }
  }

  function wireTheme() {
    var btn = document.querySelector('.themetoggle');
    if (!btn) return;
    label(btn);

    // On first load, a framed diagram has already read whatever
    // 'archify-theme' said before this page decided its own theme (the
    // default, or a stored 'theme' value); bring the diagram into line with
    // where this page actually started.
    syncDiagramTheme(root.getAttribute('data-theme') === 'light' ? 'light' : 'dark');

    btn.addEventListener('click', function () {
      var next = root.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
      root.setAttribute('data-theme', next);
      remember(next);
      label(btn);
      syncDiagramTheme(next);
    });
  }

  /* ----------------------------------------------------------- sorting */

  // Sorts on the number inside a cell when there is one, on its text when
  // there is not. Reading the number out of the text rather than requiring a
  // data attribute means a table added later sorts without being told how.
  function cellValue(row, index) {
    var cell = row.children[index];
    if (!cell) return '';
    var text = cell.textContent.trim();
    var num = parseFloat(text.replace(/[^0-9.\-]/g, ''));
    return (text !== '' && !isNaN(num)) ? num : text.toLowerCase();
  }

  function wireSorting(table) {
    var headers = table.querySelectorAll('th[data-sort]');
    Array.prototype.forEach.call(headers, function (th, i) {
      var index = Array.prototype.indexOf.call(th.parentNode.children, th);
      th.setAttribute('tabindex', '0');
      th.setAttribute('role', 'button');

      function sort() {
        var body = table.tBodies[0];
        if (!body) return;
        var asc = th.getAttribute('aria-sort') !== 'ascending';

        Array.prototype.forEach.call(headers, function (other) {
          other.removeAttribute('aria-sort');
        });
        th.setAttribute('aria-sort', asc ? 'ascending' : 'descending');

        var rows = Array.prototype.slice.call(body.rows);
        rows.sort(function (a, b) {
          var x = cellValue(a, index), y = cellValue(b, index);
          if (x < y) return asc ? -1 : 1;
          if (x > y) return asc ? 1 : -1;
          return 0;
        });
        rows.forEach(function (r) { body.appendChild(r); });
      }

      th.addEventListener('click', sort);
      th.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); sort(); }
      });
    });
  }

  /* ------------------------------------------------------------ ready */

  function init() {
    wireTheme();
    Array.prototype.forEach.call(
      document.querySelectorAll('table[data-sortable]'), wireSorting);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
