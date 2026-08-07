#include "../common/chat_server.hpp"

#include <cerrno>
#include <poll.h>
#include <unordered_map>
#include <vector>

class PollDispatcher final : public ReadinessDispatcher {
public:
    const char *name() const override { return "poll"; }

    bool add(int fd) override {
        indexes[fd] = descriptors.size();
        descriptors.push_back({fd, POLLIN, 0});
        return true;
    }

    bool remove(int fd) override {
        auto found = indexes.find(fd);
        if (found == indexes.end()) {
            errno = ENOENT;
            return false;
        }
        size_t index = found->second;
        int movedFd = descriptors.back().fd;
        descriptors[index] = descriptors.back();
        descriptors.pop_back();
        indexes.erase(found);
        if (index < descriptors.size()) {
            indexes[movedFd] = index;
        }
        return true;
    }

    bool setWriteInterest(int fd, bool enabled) override {
        auto found = indexes.find(fd);
        if (found == indexes.end()) {
            errno = ENOENT;
            return false;
        }
        pollfd &descriptor = descriptors[found->second];
        if (enabled) {
            descriptor.events |= POLLOUT;
        } else {
            descriptor.events &= ~POLLOUT;
        }
        return true;
    }

    int wait(std::vector<ReadyEvent> &events) override {
        lastMaxEvents = descriptors.size();
        int count = poll(descriptors.data(), descriptors.size(), -1);
        if (count <= 0) {
            events.clear();
            return count;
        }
        events.clear();
        events.reserve(static_cast<size_t>(count));
        for (const pollfd &descriptor : descriptors) {
            if (descriptor.revents == 0) {
                continue;
            }
            uint32_t flags = 0;
            if (descriptor.revents & POLLIN) flags |= READY_READ;
            if (descriptor.revents & POLLOUT) flags |= READY_WRITE;
            if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) flags |= READY_ERROR;
            events.push_back({descriptor.fd, flags});
        }
        return count;
    }

    size_t maxEvents() const override { return lastMaxEvents; }

private:
    std::vector<pollfd> descriptors;
    std::unordered_map<int, size_t> indexes;
    size_t lastMaxEvents = 0;
};

int main() {
    PollDispatcher dispatcher;
    return runChatServer(dispatcher);
}
