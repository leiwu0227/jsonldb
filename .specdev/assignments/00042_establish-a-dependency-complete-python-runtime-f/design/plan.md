# Plan

Provision and qualify one ignored, repository-scoped Python 3.12 environment
from declaration-compatible distributions in the local package cache, checking
the resolved set against the existing universal hash lock. Record a sourceable
PATH handoff so the parent Mission executor can run its unchanged command with
every unqualified `python3` segment resolving to the same environment.

## Knowledge used

- `.specdev/knowledge/_index.md` — confirms that living knowledge is supporting
  context rather than authority.
- `.specdev/knowledge/workflow/adhoc-history.md` — no applicable prior Adhoc
  runtime handoff was identified; historical receipts would require current
  verification before reuse.
- `.specdev/knowledge/workflow/wsl2-filesystem.md` — reviewed and excluded
  because this Attempt runs on macOS, as the approved contract states.
- `.specdev/project_notes/big_picture.md` — confirms Python >=3.8 and the six
  declared runtime dependency families.
- `.specdev/assignments/00040_add-dependency-backed-runtime-coverage-for-versi/outcome.md`
  and `.specdev/assignments/00041_make-dependency-backed-logging-regressions-colle/outcome.md`
  — establish the prior compatible Python 3.12 dependency-backed test context,
  while not substituting for a fresh repository-scoped runtime.

Fresh searches:

- `specdev knowledge search "repository scoped python runtime dependencies PATH handoff"`
- `specdev knowledge search "uv offline cache operation not permitted sandbox"`
  after the global uv cache's `sdists-v9/.git` sentinel produced an unexpected
  sandbox permission failure. No applicable guidance was found, so current
  package-manager evidence governs the repository-local cache-view workaround.

**Implementation Guides:** []

**Review Guides:** []

The available `frontend` and `api-security` guides are outside this isolated
runtime-provisioning scope, so the smallest useful guide set is empty.

## Tasks

1. **T-1 (AC-1):** Create or refresh an ignored repository-local Python 3.12
   virtual environment under `.specdev/cache/`, use a repository-local cache
   view and copy mode to install locally available declaration-compatible
   dependencies without retaining a dependency on the cache view, changing
   global Python, or changing tracked dependency files, and record a
   sourceable PATH handoff for the parent Mission executor. Compare the
   resolved environment against the existing universal hash lock and record
   any unavoidable locally-cached version divergence.
2. **T-2 (AC-1):** With the recorded handoff active, verify unqualified
   `python3` resolves inside the repository runtime, meets the >=3.8 floor, and
   imports pandas, orjson, NumPy, GitPython, Bokeh, and Matplotlib from that
   environment; run the ecosystem consistency check and record resolved
   versions.
3. **T-3 (AC-2):** With the same handoff, run only the authorized collection
   probe, confirm collection has no missing-dependency errors, and confirm the
   real GitPython, Bokeh, and Matplotlib regression test nodes are present.
4. **T-4 (AC-1, AC-2):** Write exact progress receipts and the concise outcome,
   including any deviations or unresolved risks, without running the
   Mission-reserved full suite or frozen integrated command.
