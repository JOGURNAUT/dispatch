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

  function wireTheme() {
    var btn = document.querySelector('.themetoggle');
    if (!btn) return;
    label(btn);
    btn.addEventListener('click', function () {
      var next = root.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
      root.setAttribute('data-theme', next);
      remember(next);
      label(btn);
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
