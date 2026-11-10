# Storage audit — latest update November 8, 2026

The prior November 7 inventory below is historical. See the [November 8 archive](../benchmarks/2026-11-08/README.md) and its cleanup journals for current generated-data removal, retained sources and rebuild recipes.

# Storage audit — November 7, 2026

Initial allocated size was approximately 84 GiB (`du`); results used 49 GiB.
Cleanup removed 4,167,090,176 allocated bytes (3.88 GiB). The JSON journal
beside this document records every removed file, fixture SHA-256 hashes,
binary fixture metadata, symlink destinations and regeneration commands.
Regenerate brinson-15k before brinson-multi. Timings after regeneration need
fresh measurements; the journal does not establish byte-identical regeneration
across dependency versions. Historical reports and raw result outputs remain.

Removed: old data/100k binary fixture, data/brinson-15k and data/brinson-multi
CSV fixtures, and rust/target/debug. Release executables were retained.

## Retained dependencies and further opportunities

- results/large_parquet/securities.parquet: 42.25 GiB, a shared analytics input,
  not a duplicate output. Six-input datasets link to it. Recreate with
  `.venv/bin/python python/benchmark_large_parquet.py generate`; retain
  generate.json and scan.json. Deleting it prevents full six-input workloads
  until regeneration. A manifest or hash cannot replace row-level analysis.
- attribution_data_parquet (~4.9 GiB) and attribution_data (~12 GiB): paired
  Parquet/CSV inputs. CSV holdings can be exported again using
  python/export_attribution_csvs.py, but historical SMF distinctions matter.
  Current and historical scripts link into these folders; do not delete them
  without migrating dependencies and preserving effective-dated SMF history,
  remapping and expansion provenance.
- data/brinson-100k and data/brinson-multi-100k (~9.6 GiB combined): older CSV
  schema includes inline segment_id; available generators do not directly
  reproduce this contract. Retain pending verification of the original recipe.
- .git (~3.6 GiB): preserve repository history. Inspect Git object retention
  separately; do not delete objects manually.
- results/native-benchmark and results/native-full-history (~3.3 GiB): raw
  historical attribution outputs, not duplicate inputs. Retained for detailed
  reconciliation. Summary metrics alone cannot support all future analyses.
- results/six-input (~2 GiB): snapshot and daily-heavy workloads;
  shared symlinks already avoid copies. Retained with their source dependencies.
- .venv (~406 MiB) and release build artifacts (~635 MiB): rebuildable, but
  retained to keep current runs usable.

The small vc_gen_attribution_data supplied inputs and their derived API evidence
were retained. No source code, reports, metrics or supplied data were deleted.

`python3 python/cleanup_old_fixtures.py` inventories only; `--apply` removes
only the fixed obsolete paths above and refuses external symlink dependencies.
Do not run it after intentionally regenerating those fixtures for active work.
