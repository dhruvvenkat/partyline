# Partyline: An experiment in Linux I/O event notifications

Partyline is a Linux-native TCP chatroom-style server implemented using two methods of I/O event notification:
 - `poll(2)`: The most basic polling method (derived from `select(2)`), uses O(N) scans over all socket connections to see which ones have incoming/outgoing messages
 - `epoll(7)`: A more advanced polling method that persists a list of sockets of interest in the kernel and returns them in batches

## Base features
Each version of the chat server keeps these primary functions/features as a control; the only thing that changes between each implementation is the I/O event notification mechanism:
 - Single-threadedness to force concurrency to come from readiness multiplexing
 - Partial reads and writes handled via nonblocking sockets to prevent the server from getting clogged up by high-latency requests
 - Newline-delimited message frames for clear separation of client messages
 - Hard limits to the number of frames and bytes that can be processed from one socket connection to prevent spam from hammering the server
 - Per-client bounded outbound queue that disconnects the client on overflow; the queue is drained as bytes are written to the on-server buffer
 - Client-created and joinable rooms
 - Room-scoped message broadcast to restrict messages to those in the same room as the sender

## What I'm measuring
The main things I'm measuring in terms of performance are:
 - p50, p95, p99 latency
 - Server CPU utilization
 - p95 scheduling lag, disconnects, and send errors
 - Readiness batch sizes, socket/control syscalls, queue high-water marks, and generator validity

See [`bench/README.md`](bench/README.md) for commands, validity rules, result
schema, workload separation, diagnostics, and reproduction steps. Historical
results are retained but explicitly incompatible with the current harness.
