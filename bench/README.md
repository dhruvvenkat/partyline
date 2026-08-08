# Benchmark harness

The current harness compares the release `poll(2)` and `epoll(7)` servers with
the same application core and a process-based load generator. Each worker
process owns a stable client partition and selector, so Python's GIL is not in
the network hot path.

Historical CSV/XLSX files in the backend result folders predate this harness
and are not comparable. See [HISTORICAL_RESULTS.md](HISTORICAL_RESULTS.md).

## Build and test

```sh
make release          # both servers: -O2 -DNDEBUG
make debug            # separate unoptimized debug binaries
make asan             # separate ASan/UBSan binaries
make test             # unit, integration, parity, fairness, and headroom tests
```

The active server implementation is shared except for
`poll(2)/poll_dispatcher.cpp` and `epoll/epoll_dispatcher.cpp`. This keeps the
protocol, room behavior, limits, immediate writes, fairness budgets,
backpressure, and command handling identical.

## Comparison commands

Every command creates a new timestamped result directory under
`bench/results/`; historical directories are never reused or removed.

```sh
make compare-smoke
make compare-sparse
make compare-dense
make compare-broadcast
make compare           # all three full default sweeps
```

The full driver can be configured directly:

```sh
python3 bench/compare.py \
  --sparse-tiers 1000,5000,10000,50000 --sparse-active 1,10,100 \
  --sparse-rate 1000 \
  --dense-tiers 100,500,1000 --dense-rates 1000,5000,10000,25000,50000,100000 \
  --broadcast-tiers 10,100,500 --broadcast-rates 100,500,1000,5000 \
  --runs 3 --duration 10 --warmup 1 --drain 2 --workers 4
```

The driver builds both release binaries unless `--no-build` is given, passes
absolute server paths with correct labels, and reverses poll/epoll order on
alternating trials. Dense and broadcast rates are swept independently for
each server; higher rates are skipped after that implementation first becomes
invalid.

Optional CPU isolation uses ordinary per-process affinity and needs no root:

```sh
python3 bench/compare.py --server-cpu 0 --worker-cpus 2,4,5,6
```

The requested and observed affinity status is recorded in both metadata and
trial rows. Use different physical cores for the server and workers.

## Experiments

### Sparse readiness

`total_connections` and `active_connections` are independent. Inactive
clients remain connected at the username prompt and produce no measured
traffic. Active clients issue direct `WHERE` requests at one fixed global
offered rate. The default totals are 1K, 5K, 10K, and 50K, with 1, 10, and 100
active clients where possible.

For localhost tests, verify `/proc/sys/net/ipv4/ip_local_port_range` before a
large tier. One connection consumes one client source port, so a 50K tier
requires at least 50K usable source tuples; lower the tier or run the generator
from additional hosts when the local range is smaller. Do not change
machine-wide networking settings just for a benchmark.

Headline fields include CPU per completed request, p50/p95/p99 latency, wait
calls, events per wait, full event batches, `epoll_ctl` calls, and generator
validity.

### Dense readiness

All connected clients are active. Each server is swept through ascending
global offered rates until latency, deadline completion, delivery, scheduling,
or error criteria fail. `summary.json` reports the highest rate that completed
all requested trials validly; a single fixed-rate run is never reported as
maximum throughput.

### Broadcast and backpressure

Broadcast rows are kept out of the readiness-dispatch headline comparison.
They record inbound messages/s, outbound deliveries/s, per-socket receive
rate, fan-out, queue high-water marks, disconnects, and generator service
gaps. The server's normal 16 KiB per-client queue is retained. No larger
dispatcher-isolation queue configuration is currently used.

## Generator behavior

Workers share an absolute monotonic start time. The offered rate is global,
not per-client. Each selector loop handles at most 32 overdue offers before
servicing reads. Offers over the scheduling-lag limit are counted as missed
instead of being injected as a burst. Client-side partial nonblocking sends
are buffered and retried in order.

Each ready socket is rotated through the batch and bounded to four `recv()`
calls and 64 KiB per selector iteration. Trial rows include generator CPU,
p50/p95/p99 scheduling lag, missed offers, worker start skew, partial client
sends, and maximum delay between a selector return and read service.

Warmup rate is capped independently and defaults to 1K/s, but never exceeds
the measured offered rate. Use `--warmup-rate` to lower the cap. This prevents
an overload probe or low-rate fan-out trial from disconnecting clients before
interval counters and validity checks begin.

## Validity

Every row contains `valid` and `invalid_reasons`. By default a normal trial is
invalid when it has any of the following:

- an unexpected client/server disconnect or send error;
- delivery ratio below 0.999;
- completed-by-deadline ratio below 0.99;
- missed-offer ratio above 0.01;
- scheduler p99 or maximum read-service delay above 20 ms;
- latency p99 above 50 ms;
- a server crash or harness error;
- missing server instrumentation;
- output queue overflow outside an explicitly allowed backpressure diagnostic.

Thresholds are command-line options. `headline-valid-medians.csv` contains
only valid rows. `invalid-trials.csv` lists rejected rows and reasons.
`summary.json` contains valid/invalid counts and sustainable dense/broadcast
rates.

## Result schema and metadata

`results.csv` records:

- identity: experiment, implementation label, server path, run, totals,
  active clients, workers, workload, payload, and affinity status;
- offered work: scheduled operations, sent operations, missed offers, and
  global offered rate;
- completion: completed-by-deadline, eventual completions, deliveries,
  delivery ratios, fan-out, and inbound/outbound rates;
- latency and driver health: p50/p95/p99, scheduling-lag percentiles,
  generator CPU, server CPU, CPU/completion, and read-service gap;
- server interval counters: wait calls, total events, p50/p95/max events per
  wait, full event batches, `epoll_ctl` ADD/MOD/DEL, immediate and partial
  writes, send/recv/EAGAIN counts, bytes, disconnects, queue overflow, and
  queue high-water marks;
- validity: crash/error/disconnect fields, `valid`, and `invalid_reasons`.

Counters are accumulated in memory between `SIGUSR1` and `SIGUSR2`; the server
writes the snapshot only after the timed interval. The harness sets
`CHAT_LOG_LEVEL=off`, so per-event logging is not part of benchmark timing.

`metadata.json` records the full command, git SHA and dirty status,
compiler/flags, kernel, CPU, file-descriptor limits, harness version, worker
count, requested affinity, workload parameters, absolute server paths,
execution order, skipped rates, and return codes.

## Diagnostics

`perf stat` and `strace -c` are separate diagnostics and are never eligible
for latency headlines:

```sh
make profile-perf
make profile-strace

python3 bench/profile.py --mode perf --implementation poll \
  --server-cpu 0 --worker-cpus 2,4,5,6
```

The perf wrapper requests task clock, cycles, instructions, context switches,
CPU migrations, cache references/misses, and readable syscall tracepoints for
`poll`, `epoll_wait`, `epoll_ctl`, `sendto`, and `recvfrom`. Unavailable
tracepoints are omitted. The strace wrapper attaches with `-f -c`; traced rows
are explicitly invalid for latency claims. Permission failures and
`perf_event_paranoid` are preserved in the diagnostic result directory. The
wrappers never request root or alter machine-wide settings.

## Reproduction checklist

1. Close unrelated CPU- and network-heavy programs.
2. Record `ulimit -n`; the driver also stores soft and hard limits.
3. Build with `make release` and verify `make test`.
4. Choose non-overlapping physical cores if using affinity.
5. Run at least three trials; order alternation is automatic.
6. Read `invalid-trials.csv` before using headline medians.
7. Compare only rows from the same harness version, commit, flags, workload,
   thresholds, and machine configuration.
