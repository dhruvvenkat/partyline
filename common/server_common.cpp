#include "server_common.hpp"

#include <algorithm>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <sstream>

namespace {
ServerMetrics metrics;
uint64_t serverIteration = 1;
volatile std::sig_atomic_t metricsStartRequested = 0;
volatile std::sig_atomic_t metricsStopRequested = 0;

void metricsSignalHandler(int signalNumber) {
    if (signalNumber == SIGUSR1) {
        metricsStartRequested = 1;
    } else if (signalNumber == SIGUSR2) {
        metricsStopRequested = 1;
    }
}

void appendMetrics(const std::string &line) {
    const char *path = std::getenv("CHAT_METRICS_FILE");
    if (path == nullptr || *path == '\0') {
        return;
    }
    std::ofstream output(path, std::ios::app);
    if (output) {
        output << line << '\n';
    }
}
}

void ServerMetrics::reset() {
    const bool wasActive = active;
    *this = ServerMetrics{};
    active = wasActive;
}

void ServerMetrics::recordWait(size_t ready, size_t maxevents) {
    if (!active) {
        return;
    }
    waitCalls++;
    totalEvents += ready;
    if (ready == maxevents) {
        fullEventBatches++;
    }
    if (eventsPerWait.size() <= ready) {
        eventsPerWait.resize(ready + 1);
    }
    eventsPerWait[ready]++;
}

uint64_t ServerMetrics::eventsPercentile(double fraction) const {
    if (waitCalls == 0) {
        return 0;
    }
    const uint64_t target = std::max<uint64_t>(
        1, static_cast<uint64_t>(fraction * static_cast<double>(waitCalls) + 0.999999));
    uint64_t seen = 0;
    for (size_t value = 0; value < eventsPerWait.size(); value++) {
        seen += eventsPerWait[value];
        if (seen >= target) {
            return value;
        }
    }
    return maxEventsPerWait();
}

uint64_t ServerMetrics::maxEventsPerWait() const {
    for (size_t value = eventsPerWait.size(); value > 0; value--) {
        if (eventsPerWait[value - 1] != 0) {
            return value - 1;
        }
    }
    return 0;
}

ServerMetrics &serverMetrics() {
    return metrics;
}

void beginServerIteration() {
    serverIteration++;
}

size_t &clientWriteBudget(ClientConnection *client) {
    if (client->writeBudgetIteration != serverIteration) {
        client->writeBudgetIteration = serverIteration;
        client->writeBudgetRemaining = MAX_BYTES_WRITTEN_PER_POLL;
    }
    return client->writeBudgetRemaining;
}

void installMetricsSignalHandlers() {
    struct sigaction action{};
    action.sa_handler = metricsSignalHandler;
    sigemptyset(&action.sa_mask);
    sigaction(SIGUSR1, &action, nullptr);
    sigaction(SIGUSR2, &action, nullptr);
}

void serviceMetricsSignals() {
    if (metricsStartRequested) {
        metricsStartRequested = 0;
        metrics.active = false;
        metrics.reset();
        metrics.active = true;
        appendMetrics("{\"status\":\"started\"}");
    }
    if (!metricsStopRequested) {
        return;
    }

    metricsStopRequested = 0;
    metrics.active = false;
    std::ostringstream output;
    output << "{\"status\":\"stopped\""
           << ",\"wait_calls\":" << metrics.waitCalls
           << ",\"total_events\":" << metrics.totalEvents
           << ",\"events_per_wait_p50\":" << metrics.eventsPercentile(0.50)
           << ",\"events_per_wait_p95\":" << metrics.eventsPercentile(0.95)
           << ",\"events_per_wait_max\":" << metrics.maxEventsPerWait()
           << ",\"full_event_batches\":" << metrics.fullEventBatches
           << ",\"epoll_ctl_add\":" << metrics.epollCtlAdd
           << ",\"epoll_ctl_mod\":" << metrics.epollCtlMod
           << ",\"epoll_ctl_del\":" << metrics.epollCtlDel
           << ",\"immediate_write_successes\":" << metrics.immediateWriteSuccesses
           << ",\"partial_writes\":" << metrics.partialWrites
           << ",\"send_calls\":" << metrics.sendCalls
           << ",\"recv_calls\":" << metrics.recvCalls
           << ",\"send_eagain\":" << metrics.sendEagain
           << ",\"recv_eagain\":" << metrics.recvEagain
           << ",\"bytes_read\":" << metrics.bytesRead
           << ",\"bytes_written\":" << metrics.bytesWritten
           << ",\"queue_overflows\":" << metrics.queueOverflows
           << ",\"queue_high_water_bytes\":" << metrics.queueHighWaterBytes
           << ",\"queue_high_water_messages\":" << metrics.queueHighWaterMessages
           << '}';
    appendMetrics(output.str());
}
