#include "common/server_common.hpp"

#include <cassert>
#include <cerrno>
#include <fcntl.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

static void setNonblocking(int fd) {
    assert(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) == 0);
}

int main() {
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    setNonblocking(sockets[0]);
    setNonblocking(sockets[1]);

    ClientConnection client{};
    client.fd = sockets[0];
    int arms = 0;
    int disarms = 0;
    auto setInterest = [&](bool enabled) {
        enabled ? arms++ : disarms++;
        return true;
    };

    const std::string immediate = "immediate";
    assert(queueOutputCommon(&client, immediate.data(), immediate.size(), setInterest));
    assert(client.outputQueue.empty());
    assert(arms == 0 && disarms == 0);

    char buffer[65536];
    assert(recv(sockets[1], buffer, sizeof buffer, 0) ==
           static_cast<ssize_t>(immediate.size()));

    std::string fill(4096, 'x');
    while (send(sockets[0], fill.data(), fill.size(), MSG_NOSIGNAL) > 0) {
    }
    assert(errno == EAGAIN || errno == EWOULDBLOCK);

    const std::string first = "first";
    const std::string second = "second";
    assert(queueOutputCommon(&client, first.data(), first.size(), setInterest));
    assert(client.writeInterestEnabled);
    assert(arms == 1 && disarms == 0);
    assert(queueOutputCommon(&client, second.data(), second.size(), setInterest));
    assert(arms == 1 && disarms == 0);

    while (recv(sockets[1], buffer, sizeof buffer, 0) > 0) {
    }
    assert(errno == EAGAIN || errno == EWOULDBLOCK);
    assert(flushOutputCommon(&client, setInterest));
    assert(client.outputQueue.empty());
    assert(!client.writeInterestEnabled);
    assert(arms == 1 && disarms == 1);
    assert(flushOutputCommon(&client, setInterest));
    assert(disarms == 1);

    close(sockets[0]);
    close(sockets[1]);
}
