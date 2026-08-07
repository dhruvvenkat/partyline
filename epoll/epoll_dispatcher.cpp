#include "../common/chat_server.hpp"
#include "../common/server_common.hpp"

#include <cerrno>
#include <sys/epoll.h>
#include <unistd.h>
#include <vector>

constexpr int MAX_EVENTS = 4096;

class EpollDispatcher final : public ReadinessDispatcher {
public:
    EpollDispatcher() : epollFd(epoll_create1(EPOLL_CLOEXEC)), returned(MAX_EVENTS) {}
    ~EpollDispatcher() override {
        if (epollFd != -1) {
            close(epollFd);
        }
    }

    bool valid() const { return epollFd != -1; }
    const char *name() const override { return "epoll"; }

    bool add(int fd) override {
        epoll_event event{};
        event.data.fd = fd;
        event.events = EPOLLIN | EPOLLRDHUP;
        if (serverMetrics().active) serverMetrics().epollCtlAdd++;
        return epoll_ctl(epollFd, EPOLL_CTL_ADD, fd, &event) == 0;
    }

    bool remove(int fd) override {
        if (serverMetrics().active) serverMetrics().epollCtlDel++;
        return epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr) == 0;
    }

    bool setWriteInterest(int fd, bool enabled) override {
        epoll_event event{};
        event.data.fd = fd;
        event.events = EPOLLIN | EPOLLRDHUP |
                       (enabled ? static_cast<uint32_t>(EPOLLOUT) : 0U);
        if (serverMetrics().active) serverMetrics().epollCtlMod++;
        return epoll_ctl(epollFd, EPOLL_CTL_MOD, fd, &event) == 0;
    }

    int wait(std::vector<ReadyEvent> &events) override {
        int count = epoll_wait(epollFd, returned.data(), returned.size(), -1);
        if (count <= 0) {
            events.clear();
            return count;
        }
        events.clear();
        events.reserve(static_cast<size_t>(count));
        for (int index = 0; index < count; index++) {
            uint32_t flags = 0;
            if (returned[index].events & EPOLLIN) flags |= READY_READ;
            if (returned[index].events & EPOLLOUT) flags |= READY_WRITE;
            if (returned[index].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) flags |= READY_ERROR;
            events.push_back({returned[index].data.fd, flags});
        }
        return count;
    }

    size_t maxEvents() const override { return returned.size(); }

private:
    int epollFd;
    std::vector<epoll_event> returned;
};

int main() {
    EpollDispatcher dispatcher;
    if (!dispatcher.valid()) {
        return 1;
    }
    return runChatServer(dispatcher);
}
