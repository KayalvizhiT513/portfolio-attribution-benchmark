# Optional input-cache preparation (Go and Rust)

Existing behavior is unchanged by default. The native Python benchmark runners
that call `benchmark_native.measured` now accept `BENCHMARK_INPUT_CACHE`:

- `normal`: leave cache state unchanged (this does not guarantee warm inputs).
- `evict`: Linux only; fsync each input then request whole-file eviction with
  `POSIX_FADV_DONTNEED`. Includes security master, index, every portfolio and any
  `--instruments-file`, resolving symlinks. This is advisory, not verified eviction.
- `macos-purge`: macOS only; invokes `sudo -n /usr/sbin/purge` before **each**
  native process. This clears the **system-wide disk cache**, affecting other apps;
  it is not selective per-file eviction. Run `sudo -v` separately first. Missing
  privileges or command errors stop the run instead of silently using cached data.

For example, on Mac, after building both native binaries:

```bash
sudo -v
BENCHMARK_INPUT_CACHE=macos-purge python3 python/benchmark_country_runtime.py --output-dir results/country-cold
```

On Linux:

```bash
BENCHMARK_INPUT_CACHE=evict python3 python/benchmark_country_runtime.py --output-dir results/country-cold
```

Preparation happens after input generation/verification and immediately before each
Go or Rust invocation, outside its measured wall time. Usage records include the
mode, preparation duration, resolved input paths and `cache_eviction_verified=false`.
Preparation repeats even if an earlier run or output validation warmed the inputs.
Keep benchmark runs sequential; another process can repopulate the cache.

For a direct invocation of either native binary, use the common wrapper:

```bash
python3 python/input_cache.py --cache evict -- ./benchmark-go-attribution --data attribution_data --scenario country --output-dir results/go-cold
python3 python/input_cache.py --cache evict -- ./rust/target/release/portfolio-attribution --data attribution_data --scenario country --output-dir results/rust-cold
```

Use `--cache macos-purge` on Mac after `sudo -v`. Native binaries invoked without
this wrapper do not perform cache preparation. The wrapper is for these attribution
commands, not arbitrary programs. Use a separate output directory for each run.

These modes approximate cold OS-cache conditions; they do not flush SSD controller
caches or promise that every byte comes from flash. Reads can still benefit from
readahead and data cached earlier within the same invocation. Production also uses
the OS cache, so compare normal and cold runs rather than treating cold as its only
representative workload. Full production-sized performance runs require the user's
input datasets; unit checks do not establish production timing.
