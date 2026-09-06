# Implementation plan

**Implementation Guides:** none
**Review Guides:** none

## T-1 — Canonical placement and recoverable moves (AC-1, AC-2, AC-3, AC-4)

Update `jsonldb/folderdb.py` to exclude the leaf from directory placement and
remove minimum-depth validation. Share visible-table scanning and collision
preflight between open migration, hierarchy lint, and quarantine restoration.
Use a hidden, atomic pending-move record to retain intended settings and all
source/destination pairs across interruptions, including index-only unfinished
moves. Resume it before inference; publish controls and refreshed statistics only
after moves. Preserve existing controls on preflight failure. Use deepest
observed depth for missing-control inference, recording that it is a fallback.
Keep the module within 1250 lines by replacing duplicated move loops.

## T-2 — Focused behavior and failure evidence (AC-1, AC-2, AC-3, AC-4)

Update existing hierarchy path expectations and recovery cases. Add tests for
mixed short/exact/long names, omitted/unchanged-depth legacy migration, controls,
bytes and indexes, collision preflight, unsafe paths, and interrupted retries for
migration, reorganization, and explicit restoration. Run focused pytest modules
through `.specdev/cache/bin/python-runtime-env python3`.

## T-3 — Documentation and delivery (AC-5)

Update affected docs and portable-dataset examples, verify source line cap and
notebook structure, publish acceptance evidence, and run the required SpecDev
implementation review. Repair concrete findings and finish the delivery commit.
Preserve independent Discussion state. Include the user's preceding big-picture
correction as authorized documentation context; remove its pending-implementation
sentence once the feature is delivered.
