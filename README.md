# Partyline: An Event-Driven TCP Chat Server

A C++ TCP chat server built around a single `poll()` event loop. It accepts multiple clients, tracks per-client state, and broadcasts messages without dedicating a thread to each connection.

The project is being developed toward a nonblocking, backpressure-aware server that remains responsive when clients are slow or stop reading.

## Roadmap

- Per-client state, usernames, rooms, input buffers, and output queues
- Nonblocking sockets and event-driven username handling
- Newline-delimited commands such as `NAME`, `JOIN`, `LEAVE`, `MSG`, and `QUIT`
- Room-based broadcasting
- Partial-write handling with bounded pending output queues
- Slow-client protection and event-loop fairness
- Metrics, stress testing, and required edge-case tests
- Optional `epoll` support after the `poll()` implementation is complete

The implementation goal is a single event-loop thread that handles fragmented messages, partial writes, slow clients, and clean connection shutdown without allowing one client to stall the server.
