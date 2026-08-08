# Stored comparison-v2 results

These directories are immutable outputs from the repaired process-based
harness. Historical `poll(2)/run-*` and `epoll/run-*` data uses an older,
incompatible harness; see `../HISTORICAL_RESULTS.md`.

Canonical result sets:

- `20260808T001601Z-smoke-8e3b80f`: post-drain-fix smoke comparison.
- `20260808T002010Z-sparse-final-3817fa4`: two-trial 1K/10K sparse comparison.
- `20260808T002143Z-dense-final-3817fa4`: two-trial dense rate sweep.
- `20260808T002326Z-broadcast-final-3817fa4`: two-trial broadcast sweep.
- `20260808T002705Z-broadcast-pressure-final-de2bcb3`: measured 10K/25K
  broadcast pressure boundary after separating the warmup rate.
- `20260808T002743Z-sparse-20k-de2bcb3`: strongest safe local sparse tier.

The other directories are retained because results are never overwritten:

- `20260808T001500Z-smoke-1efcc87` exposed and motivated the broadcast drain
  fix.
- `20260808T001633Z-sparse-short-8e3b80f` and
  `20260808T001813Z-sparse-confirm-8e3b80f` are superseded sparse checks.
- `20260808T002454Z-broadcast-pressure-3817fa4` exposed and motivated the
  independent warmup-rate fix.
- `20260808T002655Z-broadcast-pressure-final-de2bcb3` records a sandbox-denied
  launch with no trial rows; the successful rerun has timestamp `002705Z`.
- `20260808T002721Z-diagnostic-perf-epoll-de2bcb3` and
  `20260808T002727Z-diagnostic-strace-epoll-de2bcb3` record local profiling
  permission failures. Diagnostic runs are never eligible for latency claims.

For completed comparisons, use `metadata.json`, `summary.json`,
`headline-valid-medians.csv`, and `invalid-trials.csv` together. Headline rows
contain valid trials only.
