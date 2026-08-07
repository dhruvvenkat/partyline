#pragma once

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <sys/socket.h>
#include <vector>

constexpr char PORT[] = "1234";
constexpr int QUEUE_LENGTH = 10;
constexpr int MAX_DATA_SIZE = 256;
constexpr size_t MAX_FRAME_BYTES = 4096;
constexpr size_t MAX_INPUT_BUFFER_BYTES = 8192;
constexpr int CLIENT_AWAITING_USERNAME = 0;
constexpr int CLIENT_ACTIVE = 1;
constexpr size_t MAX_PENDING_OUTPUT_BYTES = 16384;
constexpr size_t MAX_BYTES_READ_PER_POLL = 8192;
constexpr size_t MAX_BYTES_WRITTEN_PER_POLL = 8192;
constexpr size_t MAX_PROCESSED_FRAMES_PER_POLL = 32;

struct PendingWrite {
    std::string data;
    size_t offset = 0;
};

struct ClientConnection {
    int fd;
    std::string username;
    int currRoom;
    std::string inputBuffer;
    std::deque<PendingWrite> outputQueue;
    size_t outputQueueSize;
    size_t peakPendingOutputBytes;
    int clientState;
    bool writeInterestEnabled = false;
    uint64_t writeBudgetIteration = 0;
    size_t writeBudgetRemaining = 0;
};

struct ChatRoom {
    int roomIdx;
    std::string roomName;
    std::vector<int> subscribedClients;
};

struct ServerMetrics {
    bool active = false;
    uint64_t waitCalls = 0;
    uint64_t totalEvents = 0;
    uint64_t fullEventBatches = 0;
    uint64_t epollCtlAdd = 0;
    uint64_t epollCtlMod = 0;
    uint64_t epollCtlDel = 0;
    uint64_t immediateWriteSuccesses = 0;
    uint64_t partialWrites = 0;
    uint64_t sendCalls = 0;
    uint64_t recvCalls = 0;
    uint64_t sendEagain = 0;
    uint64_t recvEagain = 0;
    uint64_t bytesRead = 0;
    uint64_t bytesWritten = 0;
    uint64_t queueOverflows = 0;
    uint64_t queueHighWaterBytes = 0;
    uint64_t queueHighWaterMessages = 0;
    std::vector<uint64_t> eventsPerWait;

    void reset();
    void recordWait(size_t ready, size_t maxevents);
    uint64_t eventsPercentile(double fraction) const;
    uint64_t maxEventsPerWait() const;
};

ServerMetrics &serverMetrics();
void installMetricsSignalHandlers();
void serviceMetricsSignals();
void beginServerIteration();
size_t &clientWriteBudget(ClientConnection *client);

inline void recordReceiveResult(ssize_t result, int errorNumber) {
    ServerMetrics &metrics = serverMetrics();
    if (!metrics.active) {
        return;
    }
    metrics.recvCalls++;
    if (result > 0) {
        metrics.bytesRead += static_cast<uint64_t>(result);
    } else if (result < 0 && (errorNumber == EAGAIN || errorNumber == EWOULDBLOCK)) {
        metrics.recvEagain++;
    }
}

template <typename SetWriteInterest>
bool queueOutputCommon(ClientConnection *client, const char *data, size_t numBytes,
                       SetWriteInterest &&setWriteInterest) {
    ServerMetrics &metrics = serverMetrics();
    if (client->outputQueueSize > MAX_PENDING_OUTPUT_BYTES ||
        numBytes > MAX_PENDING_OUTPUT_BYTES - client->outputQueueSize) {
        if (metrics.active) {
            metrics.queueOverflows++;
        }
        errno = ENOBUFS;
        return false;
    }

    size_t offset = 0;
    size_t &writeBudget = clientWriteBudget(client);
    if (client->outputQueue.empty() && numBytes != 0 && writeBudget != 0) {
        const size_t bytesToSend = std::min(numBytes, writeBudget);
        ssize_t written;
        do {
            written = send(client->fd, data, bytesToSend, MSG_NOSIGNAL);
            if (metrics.active) {
                metrics.sendCalls++;
            }
        } while (written < 0 && errno == EINTR);

        if (written > 0) {
            offset = static_cast<size_t>(written);
            writeBudget -= offset;
            if (metrics.active) {
                metrics.bytesWritten += offset;
                if (offset < bytesToSend) {
                    metrics.partialWrites++;
                }
                if (offset == numBytes) {
                    metrics.immediateWriteSuccesses++;
                }
            }
            if (offset == numBytes) {
                return true;
            }
        } else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (metrics.active) {
                metrics.sendEagain++;
            }
        } else {
            if (written == 0) {
                errno = EPIPE;
            }
            return false;
        }
    }

    if (offset < numBytes) {
        client->outputQueue.push_back({std::string(data + offset, numBytes - offset), 0});
        client->outputQueueSize += numBytes - offset;
        client->peakPendingOutputBytes = std::max(client->peakPendingOutputBytes,
                                                  client->outputQueueSize);
        if (metrics.active) {
            metrics.queueHighWaterBytes = std::max<uint64_t>(
                metrics.queueHighWaterBytes, client->outputQueueSize);
            metrics.queueHighWaterMessages = std::max<uint64_t>(
                metrics.queueHighWaterMessages, client->outputQueue.size());
        }
    }

    if (!client->outputQueue.empty() && !client->writeInterestEnabled) {
        if (!setWriteInterest(true)) {
            return false;
        }
        client->writeInterestEnabled = true;
    }
    return true;
}

template <typename SetWriteInterest>
bool flushOutputCommon(ClientConnection *client, SetWriteInterest &&setWriteInterest) {
    ServerMetrics &metrics = serverMetrics();
    size_t &writeBudget = clientWriteBudget(client);
    while (!client->outputQueue.empty() && writeBudget != 0) {
        PendingWrite &pending = client->outputQueue.front();
        const size_t bytesRemaining = pending.data.size() - pending.offset;
        const size_t bytesToSend = std::min(bytesRemaining, writeBudget);
        ssize_t written;
        do {
            written = send(client->fd, pending.data.data() + pending.offset,
                           bytesToSend, MSG_NOSIGNAL);
            if (metrics.active) {
                metrics.sendCalls++;
            }
        } while (written < 0 && errno == EINTR);

        if (written > 0) {
            const size_t count = static_cast<size_t>(written);
            pending.offset += count;
            client->outputQueueSize -= count;
            writeBudget -= count;
            if (metrics.active) {
                metrics.bytesWritten += count;
                if (count < bytesToSend) {
                    metrics.partialWrites++;
                }
            }
            if (pending.offset == pending.data.size()) {
                client->outputQueue.pop_front();
            }
            continue;
        }

        if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (metrics.active) {
                metrics.sendEagain++;
            }
            break;
        }
        if (written == 0) {
            errno = EPIPE;
        }
        return false;
    }

    if (client->outputQueue.empty() && client->writeInterestEnabled) {
        if (!setWriteInterest(false)) {
            return false;
        }
        client->writeInterestEnabled = false;
    }
    return true;
}
