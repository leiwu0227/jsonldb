# Outcome

## Delivered behavior

JSONLDB now contains only its storage, DataFrame, folder database, metadata-slot,
lint, and integrity-report modules. The Git and visualization modules are
deleted, `FolderDB` has no `commit`, `revert`, or `version` facade, and the
package inventory lists only the remaining modules. Focused regressions enforce
the missing modules and methods, the exact inventory, dependency declarations,
and core operation when imports of the removed libraries are blocked.

GitPython, Bokeh, and Matplotlib are removed from `setup.py` and the regenerated
universal lock; NumPy remains declared. Capability sections and claims are gone
from README and all five capability notebooks plus their capability-only tests
are deleted. The mixed reports suite retains its storage/report coverage.

## Deviations

The first in-place wheel reused stale ignored `build/lib` copies of both deleted
modules. In accordance with the Roadmap rule against hand-editing build
artifacts, setuptools' scoped clean command was run with the build-system's
pinned version; the next in-place wheel contained exactly the six remaining
modules and passed isolated installation. The documented lock command also
warned that no local Python 3.8 interpreter was available for building resolver
dependencies; it still emitted the requested universal Python 3.8 target, and
the separate Python 3.8 parse audit passed.

## Unresolved risks

This is an intentional breaking API removal. `big_picture.md` still describes
the old high-level inventory and remains reserved for a separate knowledge
curation workflow. `jsonlfile.py` remains exactly at its 950-line Roadmap cap.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Source and final wheel inventories contain neither module; five focused regressions prove normal missing-module and missing-attribute behavior and the reduced `__all__`. | Passed |
| AC-2 | The regenerated lock and final wheel metadata declare only pandas, orjson, and NumPy; current-surface sweeps pass after excluding the intentional negative regression and legitimate GitHub transport; the README, five notebooks, and capability-only tests are removed or rewritten. | Passed |
| AC-3 | The final wheel installed outside the checkout under isolated Python 3.12 with all three removed libraries absent and completed a FolderDB round trip; the full canonical suite passed 142 tests, 20 files passed Python 3.8 parsing, and all five remaining source caps passed. | Passed |
