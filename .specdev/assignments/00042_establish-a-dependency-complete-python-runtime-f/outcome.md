# Outcome

## Delivered behavior

An ignored repository-local Python 3.12.12 environment now contains all six
`setup.py` runtime dependencies from one consistent copied installation. The
sourceable `.specdev/cache/python-runtime-handoff.sh` selects it directly, and
the documented `.specdev/cache/bin/specdev` launcher convention carries that
PATH into subsequent Mission controller commands without changing the frozen
verification command.

## Deviations

The sandbox blocked uv's user-cache sentinel and the offline cache did not
contain every current lock pin. Provisioning therefore used a repository-local
cache view plus copy mode and declaration-compatible cached versions (notably
Bokeh 3.9.2 and GitPython 3.1.57); pytest 9.1.1 is present only as collection
tooling. No tracked dependency or product/test file changed.

## Unresolved risks

The ignored runtime is intentionally machine-local and must be reprovisioned if
`.specdev/cache` is purged. The parent must use the repository launcher or
source the recorded handoff before final verification; no additional bounded
Assignment is required.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | The handoff resolved unqualified `python3` to repository-local Python 3.12.12; all six declared dependencies imported from its copied site-packages, satisfied `setup.py`, and `uv pip check` found all 27 packages compatible. | Passed |
| AC-2 | Under the same handoff, `python3 -m pytest --collect-only -q tests` collected 123 tests and included the real GitPython and combined Bokeh/Matplotlib regression nodes. | Passed |
