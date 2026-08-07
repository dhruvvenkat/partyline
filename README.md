# Partyline: An experiment in Linux I/O evolution

Partyline is a Linux-native TCP chatroom-style server implemented using 3 different methods of I/O polling:
 - poll(2): The original polling method, uses O(N) scans over all socket connections to see which ones have incoming/outgoing messages
 - epoll(7): A more advanced polling method that returns a list of only the socket connections that have pending I/O operations in batches from the kernel
 - io_uring: A recent addition to the Linux kernel that uses a pair of circular buffers to continuously pass I/O-pending socket connections from the kernel to the application

## Base features
Each version of the chat server keeps these features as a control; the only thing that changes between each implementation is the I/O polling method:
 - Single-threadedness to force concurrency to come from readiness multiplexing
 - Partial reads and writes handled via nonblocking sockets to prevent the server from getting clogged up by high-latency requests
 - Newline-delimited message frames for clear separation of client messages
 - Hard limits to the number of frames and bytes that can be processed from one socket connection to prevent spam from hammering the server
 - Per-client bounded outbound queue that disconnects the client on overflow; the queue is drained as bytes are written to the on-server buffer
 - Client-created and joinable rooms
 - Room-scoped message broadcast to restrict messages to those in the same room as the sender

See the README file in `bench/` for specifics on the experiment.
