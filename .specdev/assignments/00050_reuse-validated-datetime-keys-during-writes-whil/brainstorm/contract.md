# Assignment contract

Kind: change

## Objective and context

Reduce datetime write latency by reusing key text produced during pre-mutation validation. Preserve public behavior and the delivered optimizations through Assignment 00049 (baseline commit `0c54d29`). Discussion D00003's `datetime_keys_design.md` and `datetime_keys_evidence.md` establish exploratory gains of 1.20–1.29x for large datetime DataFrame overwrites, 1.40x for datetime dictionary overwrites and 1.12x for large datetime upserts; these are workload-specific observations, not guaranteed ratios.

Published file-store, metadata-slot, index-cache and DataFrame-adapter designs remain authoritative. The bounded knowledge search for `datetime serialization` identified project context and Assignment 00049's outcome; preserve its eager conversion, version fallbacks and metadata/statistics behavior. Concurrent Discussion artifacts and the separately published roadmap update (`5f56588`) remain outside Assignment delivery ownership.

## Scope and non-goals

Reuse validated keys within ordinary save, atomic save and upsert, covering public dictionary/DataFrame calls and FolderDB overwrite/upsert and plural wrappers through their shared writers. Exclude read/delete conversion, changes to DataFrame materialization, streaming rows, persistent/global key caches, metadata batching, writer/index/cache redesign, new dependencies or public settings, new product modules and roadmap publication.

## Expected behavior

All rows still undergo the current record-shape and reserved-key validation before file mutation. Eligible stable key conversions are retained in input order and reused during writing. Preserve original physical row sequences and effective-index behavior when distinct timestamps or mixed keys normalize to the same string; never collapse input rows into a dictionary keyed by normalized text.

## Important decisions

- Optimize known-stable ordinary datetime and pandas Timestamp inputs; preserve existing conversion and iteration behavior for custom keys, datetime subclasses, custom timezones and mappings. Eligibility and conservative per-input/per-key fallbacks are delegated, including a datetime-leading admission rule if that avoids overhead for other workloads. Unverified supported inputs retain their existing path.
- Resolve Timestamp recognition within existing module boundaries, without a new dependency or public registration mechanism. Do not narrow supported Python/pandas versions; guard version-specific timezone/type facilities when necessary.
- Retained formatted keys are temporary per-write state. Their memory cost scales with input rows, including keys that collide; do not describe the allocation as bounded by effective index size. The measured additional allocation is an accepted design tradeoff subject to verification below.

## Constraints and invariants

Preserve public signatures and return values; datetime precision/timezones; warning and exception type, text and ordering; user-defined conversion/iteration behavior; eager DataFrame conversion; caller inputs; row/index bytes and offsets; padding, append/tombstone and collision behavior; metadata slots; synchronous per-table metadata publication; recovery, cache invalidation and existing atomic/control-file and partial-failure guarantees. No stronger durability or global concurrency guarantee is introduced.

Respect maximum total physical line counts, including blank lines, comments and docstrings: `jsonldb/jsonlfile.py` 950, `jsonldb/jsonldf.py` 150 and `jsonldb/folderdb.py` 1250. Small local consolidation needed to fit the file-store cap is delegated; unrelated refactoring and cap increases are not. Do not create a normal Assignment worktree.

## Delegated and reserved authority

After exact contract approval, delegate private helpers and conservative eligibility/fallbacks, focused tests, disposable benchmarks and compatibility environments, independent implementation review and repairs, and one final delivery commit. Use Claude Opus 5.0 at xhigh effort for review. Brainstorm review is optional; implementation review is required.

Public/design or compatibility changes, broader optimization scope, relaxed line caps, full-suite execution, review waiver, and acceptance of a material unresolved regression or missed intended performance benefit require user agreement.

## Risks and assumptions

Unrestricted reuse demonstrably changes stateful key conversion and custom mapping behavior. Fixed and named timezones, missing timestamps, supported dependency versions and per-entry-point failures need production verification beyond the prototype. Existing conversion and index I/O can dominate small updates. At 20k rows the prototype adds about 0.17 MB to full-write peak for unique timestamps and 1.37 MB when all timestamps normalize to one key; these are traced allocations, not RSS limits. Conservative admission may intentionally leave mixed or uncommon inputs unaccelerated.

## Verification authority

Focused tests, temporary storage, repeated benchmarks and isolated environments using existing supported dependency versions are allowed. Exercise relevant supported Python/pandas branches, report unavailable environments honestly and retain original behavior where equivalence evidence is insufficient. Full-suite execution requires explicit approval. Ship reproducible benchmark helpers and verify their documented commands from a fresh checkout. Use current code and the exact baseline as compatibility authority, rather than the exploratory prototype.

## Acceptance criteria

- AC-1: Public and FolderDB dictionary/DataFrame saves and upserts, atomic save and plural wrappers preserve baseline results, physical rows/indexes, slots, metadata and caller inputs. Cover ordinary datetime/Timestamp keys, precision and normalization collisions, fixed/named timezones, mixed/string/custom keys, subclasses and custom mappings, missing timestamps, empty input and representative supported-version fast/fallback paths. User-defined conversion and iteration retain their baseline observable behavior.
- AC-2: Invalid records, reserved keys, invalid precision, unsupported payloads and relevant publication failures preserve baseline exceptions/warnings and mutation timing, including pre-mutation validation, index/cache recovery and invalidation, atomic/control-file and slot guarantees, and per-table/plural metadata and partial-completion ordering.
- AC-3: Repeated alternating-order same-host comparisons against `0c54d29` show lower median complete-write latency for 100,000-row datetime DataFrame numeric/mixed overwrites, ordinary datetime dictionary overwrites and large datetime DataFrame upserts on validated paths. Report small/empty writes, small upserts, string/custom fallback costs, and separate validation/full-write peak allocation for unique and collision-heavy inputs. Document O(input rows) retention and compare the measured memory tradeoff with the exploratory evidence. No fixed twofold gain or small-upsert benefit is promised; missed intended gains or material unresolved latency/memory regressions return for a user decision.
- AC-4: The final implementation conforms to all applicable published source-file line limits. Record the measured total physical line count and limit for each targeted source file in the delivery evidence, including `jsonldb/jsonlfile.py` <= 950, `jsonldb/jsonldf.py` <= 150 and `jsonldb/folderdb.py` <= 1250. Recheck counts after implementation-review repairs and before delivery. Any exceeded limit fails acceptance and blocks completion until resolved within the approved scope or the user explicitly approves a design/contract change. Meet the caps through readable local consolidation, not code minification or moving logic outside the agreed scope.
