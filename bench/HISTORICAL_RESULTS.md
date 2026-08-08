# Historical result compatibility

The following result sets were produced by earlier harnesses and are retained
only as historical artifacts:

- `poll(2)/run-0-results/`
- `epoll/run-0-results/`
- `epoll/run-1-results/`
- `epoll/run-2-results/`
- `epoll/epoll-smoke.csv`

They used different server code, build flags, load-generator scheduling,
worker architecture, rate semantics, instrumentation, and validity rules.
They are therefore incompatible with `comparison-v2-process-validity` and
must not be combined with new rows or used for same-harness speedup claims.
The files are intentionally not deleted or overwritten.
