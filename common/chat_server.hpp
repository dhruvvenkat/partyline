#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

enum ReadyFlags : uint32_t {
    READY_READ = 1,
    READY_WRITE = 2,
    READY_ERROR = 4,
};

struct ReadyEvent {
    int fd;
    uint32_t flags;
};

class ReadinessDispatcher {
public:
    virtual ~ReadinessDispatcher() = default;
    virtual const char *name() const = 0;
    virtual bool add(int fd) = 0;
    virtual bool remove(int fd) = 0;
    virtual bool setWriteInterest(int fd, bool enabled) = 0;
    virtual int wait(std::vector<ReadyEvent> &events) = 0;
    virtual size_t maxEvents() const = 0;
};

int runChatServer(ReadinessDispatcher &dispatcher);
