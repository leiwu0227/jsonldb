# Outcome

## Delivered behavior

The ignored repository-local `python-runtime-env` launcher now exports a
validated absolute interpreter path plus a narrowly scoped `python3` Bash
function. The existing repository SpecDev wrapper delegates through it, so a
controller-spawned `/bin/sh -lc` keeps using the dependency-complete Python
3.12.12 runtime even after macOS `/etc/profile` reorders `PATH`.

## Deviations

None.

## Unresolved risks

The handoff is intentionally machine-local under ignored `.specdev/cache` and
must be reprovisioned if that cache is purged. It targets the verified macOS
`/bin/sh` implementation (Bash 3.2); replacing that shell with one that does not
import exported Bash functions would require a new repository-local handoff.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Through `python-runtime-env` and a fresh `/bin/sh -lc`, unqualified `python3` invoked the exported function, resolved to the repository Python 3.12.12 interpreter, and imported pandas, orjson, NumPy, GitPython, Bokeh, and Matplotlib from its local site-packages. A Node-mediated probe also preserved the function across the controller-shaped intermediate process. | Passed |
| AC-2 | Through the same handoff and login-shell boundary, `python3 -m pytest --collect-only -q tests` collected all 123 tests and explicitly included the GitPython and combined Bokeh/Matplotlib regression nodes. | Passed |
