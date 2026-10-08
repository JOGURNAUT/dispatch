"""Put the site nav onto the generated architecture page.

The diagram is produced by a tool that owns its own markup, so the nav cannot
live in the source it is built from. Hand-editing the output would work until
the next regeneration silently dropped it, and nobody notices a missing link --
the page still loads, it is just a dead end.

So this is a build step instead: idempotent, re-runnable, and re-run after the
diagram is regenerated. Running it twice is a no-op.

    python scripts/add_nav.py

Without it, someone who opens the architecture page from a link has no route to
the results or the source, which is the rest of the project.
"""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
TARGET = ROOT / "docs" / "dispatch-architecture.html"
MARKER = "data-dispatch-nav"

NAV = """
<style data-dispatch-nav>
  .dispatch-nav {
    position: fixed; top: 14px; left: 16px; z-index: 9999;
    display: flex; gap: 4px; align-items: center;
    font: 12.5px/1 ui-sans-serif, system-ui, "Segoe UI", sans-serif;
    background: rgba(18,18,17,.82); backdrop-filter: blur(8px);
    border: 1px solid rgba(255,255,255,.14); border-radius: 8px; padding: 4px;
  }
  .dispatch-nav a {
    color: #c3c2b7; text-decoration: none; padding: 5px 9px; border-radius: 5px;
  }
  .dispatch-nav a:hover { color: #fff; background: rgba(255,255,255,.1); }
  .dispatch-nav a[aria-current="page"] { color: #fff; background: rgba(255,255,255,.1); }
  @media (prefers-color-scheme: light) {
    .dispatch-nav { background: rgba(252,252,251,.9); border-color: rgba(0,0,0,.14); }
    .dispatch-nav a { color: #52514e; }
    .dispatch-nav a:hover, .dispatch-nav a[aria-current="page"] {
      color: #0b0b0b; background: rgba(0,0,0,.07);
    }
  }
  /* The viewer has its own controls in the top-left on narrow screens; stand
     down rather than sit on top of them. */
  @media (max-width: 760px) { .dispatch-nav { display: none; } }
</style>
<nav class="dispatch-nav" aria-label="Pages" data-dispatch-nav>
  <a href="index.html">Overview</a>
  <a href="dispatch-architecture.html" aria-current="page">Architecture</a>
  <a href="results.html">Results</a>
</nav>
"""


def main() -> int:
    if not TARGET.exists():
        sys.exit(f"no diagram at {TARGET} - generate it first")

    html = TARGET.read_text(encoding="utf-8")
    if MARKER in html:
        print(f"  nav already present in {TARGET.name}")
        return 0

    # Injected right after <body> so it is in the flow before anything the
    # viewer positions; it is fixed-position, so where it sits in the DOM only
    # matters for stacking.
    lowered = html.lower()
    at = lowered.find("<body")
    if at < 0:
        sys.exit("no <body> in the diagram - the generator's output changed shape")
    at = html.index(">", at) + 1

    TARGET.write_text(html[:at] + NAV + html[at:], encoding="utf-8")
    print(f"  nav injected into {TARGET.name}")
    print("  re-run this after regenerating the diagram")
    return 0


if __name__ == "__main__":
    sys.exit(main())
