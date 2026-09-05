---
verdict: approved
material_divergence: false
scope_divergence: none
procedure_divergence: none
evidence_integrity: complete
user_reapproval_required: false
---

## Findings

**Evidence integrity — complete.** All four receipt digests recompute exactly (`contract.md` d84e24db…, `plan.md` 7699debd…, `progress.json` 363eedba…, `outcome.md` a40387b6…). `HEAD` is still the boundary commit `5276e8e`, and the four staged/added project paths hash-match `implementation/source-hashes.json`, so the working tree is the frozen candidate. All 9 verification receipts are `authoritative_acceptance`, `passed`, at `working-tree@5276e8ea…`, with 0 omitted/superseded. No receipt was re-run and no suite was executed.

**Scope — none.** Changed paths are exactly `jsonldb/jsonldf.py`, `unit_tests/test_dataframe_conversion.py`, `profile_test/benchmark_dataframe.py`, and a one-line `.gitignore` un-ignore for the benchmark helper — the latter is required by the contract's "Ship benchmark helpers" clause, not an expansion. No excluded item (tuple specialization, lazy rows, read conversion, datetime key reuse, writer/index/cache redesign, dependencies, public settings, roadmap publication) is touched. No dependency or lockfile change exists, so no registry/advisory evidence is required.

**Contract conformance.** `_convert_df` (`jsonldb/jsonldf.py:17`) keeps conversion eager and pre-writer; `_df_records` retains the overwrite-specific `ValueError("DataFrame index must be unique")` ahead of conversion, while the upsert path still falls through to `to_dict('index')` and raises pandas' distinct `orient='index'` message — the required per-entry-point error ordering is preserved. FolderDB `overwrite_df`/`upsert_df` reach the shared helper via `folderdb.py:521/548/550`, and the public `save_jsonldf`/`update_jsonldf` via `:48/:85`, so all four contracted entry points and their plural wrappers are covered. Line caps hold: adapter 144/150, file store 950/950, FolderDB 1188/1250.

**Acceptance.** AC-1: 411 focused tests on pandas 3.0.5 plus 142 on each of 1.5.3/2.0.3/2.3.3/3.0.0; the differential test compares keys, serialized bytes, warnings, exceptions and caller-frame identity between the fast path and `to_dict('index')` across 22 shapes × both entry points. AC-2: `test_conversion_error_precedes_file_mutation` and `test_plural_conversion_failure_retains_prior_completion` establish that no conversion error reaches a partially written table, and the durability/slot/cache/byte-serialization suites pass. AC-3: I independently re-read the summaries — `100000/*/overwrite` gains are 1.2205x / 1.1395x / 1.3089x, matching the reported figures, and `check_evidence.py` re-derives medians and baseline source identity from `git show 5276e8e:`. The four sub-0.95x short-matrix samples are disclosed and each recovers under the 101-repeat dense run (10/numeric_8/overwrite 1.033x, 1000/datetime_8/upsert 0.980x, 0/mixed_8/upsert 0.986x); the worst dense result is 0.964x on empty-mixed overwrite at +0.019 ms. No material unresolved regression and no missed intended benefit, so no user decision is triggered.

**Procedure.** The pandas 1.3 branch was not runtime-tested (no usable wheel for the available Python 3.9.6). This is not a divergence: the contract directs retaining the original conversion where equivalence evidence is insufficient, the version guard does exactly that, and `test_unvalidated_version_branches_keep_original_conversion` pins it deterministically for 1.3.0 / 3.1.0 / 4.0.0. Full-suite execution was correctly not performed. The required implementation review is this pass (`review/implementation-state.json` round 0, Opus 5 @ xhigh as contracted).

Non-blocking observations, for the author's awareness only:

1. `unit_tests/test_dataframe_conversion.py:95` asserts the fast path is taken for any `pandas 3.0.x`. The product guard additionally requires `df.index.dtype.storage == 'python'`, and pandas 3.0 selects pyarrow-backed string storage by default when pyarrow is importable. In such an environment the product would correctly fall back but this assertion would fail. I did not verify this — pyarrow is absent from this runtime and is not a declared dependency, so no existing receipt is affected. Worth decoupling the assertion from the ambient storage backend in future work.
2. Consequent to the same guard, acceleration silently does not apply on pyarrow-backed pandas 3.0 installs. This narrowness is disclosed in `implementation/performance.md` and is squarely within the contract's delegation of conservative guards.

No blocking contract defect remains; every acceptance criterion has a final result.