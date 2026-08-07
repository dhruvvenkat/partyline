# Scale experiment

This benchmark is the baseline for comparing the current `poll()` server with
a future `epoll()` implementation. It launches the selected server on
loopback, creates healthy clients, warms up, measures, drains late responses,
and writes one CSV row per trial.

## Workloads

- `direct`: every client sends `WHERE`. This avoids room fan-out and measures
  request/response latency and throughput as the ready-fd set grows.
- `broadcast`: all but one client send a 128-byte marked chat message. Every
  client drains its socket. The harness counts actual outbound message
  deliveries and measures one-way latency at the observer.

`--rate` controls messages per sending client per second. The server no longer
rate-limits clients, so the saturation baseline uses the highest sustained
rate found for each tier without disconnects, send errors, or delivery loss.

The load generator partitions clients across worker threads, each with its own
selector and send schedule. It defaults to two workers; larger counts contend
on Python's GIL at this workload. Use `--workers N` to override that choice.

## Baseline

Run `make benchmark`. It performs three 10-second trials at each load and
writes the five `poll-unlimited-*.csv` files under `bench/results/`.

For a quick integration check:

```sh
make benchmark-smoke
```

## Future `epoll()` comparison

Build both binaries with identical compiler flags. On an otherwise idle
machine, alternate which implementation runs first and collect at least three
runs:

```sh
python3 bench/benchmark.py --server ./server_poll --label poll --rate 100 --workloads direct
python3 bench/benchmark.py --server ./server_epoll --label epoll --rate 100 --workloads direct
```

Compare per tier:

- p50/p95/p99 latency;
- completed inbound messages/s;
- delivered outbound messages/s and delivery ratio;
- server CPU percentage;
- p95 load-generator scheduling lag, disconnects, and send errors.

Use medians across runs. Treat results as valid only when delivery ratio is
near 1, there are no disconnects/send errors, and generator lag is small
relative to the latency being compared. Keep the machine, kernel, compiler
flags, payload, rates, and client harness unchanged.

## Unrestricted `poll()` saturation baseline: 2026-07-31

These historical numbers used the old single-threaded load generator. Rerun
both servers with the threaded harness before making implementation comparisons.

Measured on Linux 7.0.0-28-generic, Intel Core i5-1350P (12 cores/16 logical
CPUs), GCC 13.3, and Python 3.13.3. The server used the project's current
unoptimized build command.

| Workload | Users | Rate/client/s | Completed/s | Delivered/s | p50 | p95 | p99 | Server CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 10 | 100 | 1,000.0 | 1,000.0 | 0.07 ms | 0.19 ms | 0.28 ms | 4.3% |
| direct | 50 | 100 | 5,000.0 | 5,000.0 | 0.10 ms | 0.29 ms | 0.44 ms | 13.2% |
| direct | 100 | 100 | 9,999.2 | 9,999.2 | 0.12 ms | 0.27 ms | 0.49 ms | 27.3% |
| direct | 500 | 100 | 49,998.5 | 49,998.5 | 0.27 ms | 3.99 ms | 10.70 ms | 96.7% |
| broadcast | 10 | 2,000 | 17,999.9 | 161,991.4 | 0.18 ms | 3.33 ms | 7.69 ms | 39.1% |
| broadcast | 50 | 200 | 9,799.6 | 480,071.8 | 0.33 ms | 0.79 ms | 1.99 ms | 66.7% |
| broadcast | 100 | 60 | 5,939.9 | 587,964.6 | 0.29 ms | 1.89 ms | 3.75 ms | 88.2% |
| broadcast | 500 | 2 | 996.4 | 497,387.0 | 10.60 ms | 18.34 ms | 23.33 ms | 97.2% |

All rows are medians of 3/3 valid trials with 100% eventual delivery, no
disconnects, and no send errors. Overload probes found these next steps were
not sustainable: broadcast 3/client/s at 500 users, 250/client/s at 50 users,
and 3,000/client/s at 10 users. They overflowed the fixed 16 KiB per-client
pending-output queues and disconnected clients.
