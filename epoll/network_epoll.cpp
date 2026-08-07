#include "server_epoll.hpp"

#include <arpa/inet.h>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/epoll.h>

static NetworkLogLevel configuredLogLevel() {
    static const NetworkLogLevel level = [] {
        const char *configured = std::getenv("CHAT_LOG_LEVEL");
        if (configured == nullptr) {
            return NetworkLogLevel::Info;
        }

        std::string value(configured);
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        if (value == "debug") return NetworkLogLevel::Debug;
        if (value == "warn" || value == "warning") return NetworkLogLevel::Warning;
        if (value == "error") return NetworkLogLevel::Error;
        if (value == "off") return NetworkLogLevel::Off;
        return NetworkLogLevel::Info;
    }();
    return level;
}

bool networkLogEnabled(NetworkLogLevel level) {
    NetworkLogLevel configured = configuredLogLevel();
    return configured != NetworkLogLevel::Off && level >= configured;
}

void writeNetworkLog(NetworkLogLevel level, std::string_view event, const std::string &details) {
    static const char *names[] = {"debug", "info", "warn", "error", "off"};
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
    std::tm utc{};
    gmtime_r(&currentTime, &utc);

    std::ostringstream line;
    line << "timestamp=" << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
         << '.' << std::setfill('0') << std::setw(3) << milliseconds.count() << 'Z'
         << " level=" << names[static_cast<int>(level)]
         << " event=" << event;
    if (!details.empty()) {
        line << ' ' << details;
    }
    line << '\n';

    std::string output = line.str();
    std::fwrite(output.data(), 1, output.size(), stderr);
}

std::string networkLogValue(std::string_view value) {
    std::ostringstream escaped;
    escaped << '"';
    for (unsigned char ch : value) {
        switch (ch) {
            case '\\': escaped << "\\\\"; break;
            case '"': escaped << "\\\""; break;
            case '\n': escaped << "\\n"; break;
            case '\r': escaped << "\\r"; break;
            case '\t': escaped << "\\t"; break;
            default:
                if (ch < 0x20 || ch == 0x7f) {
                    escaped << "\\x" << std::hex << std::setw(2) << std::setfill('0')
                            << static_cast<int>(ch) << std::dec;
                } else {
                    escaped << static_cast<char>(ch);
                }
        }
    }
    escaped << '"';
    return escaped.str();
}

void logNetworkError(std::string_view event, int errorNumber, int fd) {
    if (fd >= 0) {
        logNetwork(NetworkLogLevel::Error, event,
                   "fd=", fd, " errno=", errorNumber,
                   " error=", networkLogValue(std::strerror(errorNumber)));
        return;
    }
    logNetwork(NetworkLogLevel::Error, event,
               "errno=", errorNumber,
               " error=", networkLogValue(std::strerror(errorNumber)));
}

const char *inet_ntop2(void *addr, char *buf, size_t size) {
    struct sockaddr_storage *sas = static_cast<sockaddr_storage *>(addr);
    struct sockaddr_in *sa4;
    struct sockaddr_in6 *sa6;
    void *src;

    switch (sas->ss_family) {
        case AF_INET:
            sa4 = static_cast<sockaddr_in *>(addr);
            src = &(sa4->sin_addr);
            break;

        case AF_INET6:
            sa6 = static_cast<sockaddr_in6 *>(addr);
            src = &(sa6->sin6_addr);
            break;

        default:
            return NULL;
    }

    return inet_ntop(sas->ss_family, src, buf, size);
}

int getListenerSocket(void) {
    int listener;
    int yes = 1;
    int rv;

    struct addrinfo hints, *ai, *p;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    const char *port = std::getenv("CHAT_SERVER_PORT");
    if (port == nullptr || *port == '\0') {
        port = PORT;
    }

    if ((rv = getaddrinfo(NULL, port, &hints, &ai)) != 0) {
        logNetwork(NetworkLogLevel::Error, "listener.resolve_failed",
                   "status=", rv, " error=", networkLogValue(gai_strerror(rv)));
        exit(1);
    }

    for (p = ai; p != NULL; p = p->ai_next) {
        listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);

        if (listener < 0) {
            logNetworkError("listener.socket_failed", errno);
            continue;
        }

        if (fcntl(listener, F_SETFL, O_NONBLOCK) == -1) {
            int errorNumber = errno;
            logNetworkError("listener.nonblocking_failed", errorNumber, listener);
            close(listener);
            continue;
        }

        if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
            logNetworkError("listener.reuseaddr_failed", errno, listener);
        }

        if (bind(listener, p->ai_addr, p->ai_addrlen)) {
            int errorNumber = errno;
            logNetworkError("listener.bind_failed", errorNumber, listener);
            close(listener);
            continue;
        }

        break;
    }

    if (p == NULL) {
        logNetwork(NetworkLogLevel::Error, "listener.unavailable", "port=", port);
        return -1;
    }

    freeaddrinfo(ai);

    if (listen(listener, QUEUE_LENGTH) == -1) {
        int errorNumber = errno;
        logNetworkError("listener.listen_failed", errorNumber, listener);
        close(listener);
        return -1;
    }

    logNetwork(NetworkLogLevel::Info, "listener.ready",
               "fd=", listener, " port=", port, " backlog=", QUEUE_LENGTH);

    return listener;
}

// Swap-and-pop to remove the relevant PFD
// void removePFD(std::vector<struct pollfd> *pfds, int i, int fdToRemove, std::map<int, int> *pfdMappings) {
//     int movedFd = pfds->back().fd;
//     (*pfds)[i] = pfds->back();
//     pfds->pop_back();
//     pfdMappings->erase(fdToRemove);

//     // Update the moved PFD's entry in the PFD mapping object
//     if (i < (int)pfds->size()) {
//         (*pfdMappings)[movedFd] = i;
//     }
// }

void disconnectClient(int epollfd, std::unordered_map<int, ClientConnection> *clients, int clientFd, const std::string &reasonForDisconnection) {

    size_t pendingBytes = 0;
    size_t peakPendingBytes = 0;
    std::string username;
    auto client = clients->find(clientFd);
    if (client != clients->end()) {
        pendingBytes = client->second.outputQueueSize;
        peakPendingBytes = client->second.peakPendingOutputBytes;
        username = client->second.username;
    }

    if (serverMetrics().active) {
        serverMetrics().epollCtlDel++;
    }
    if (epoll_ctl(epollfd, EPOLL_CTL_DEL, clientFd, nullptr) == -1) {
        logNetworkError("epoll.delete_failed", errno, clientFd);
    }

    if (close(clientFd) == -1) {
        logNetworkError("socket.close_failed", errno, clientFd);
    }
    clients->erase(clientFd);

    logNetwork(NetworkLogLevel::Info, "client.disconnected",
               "fd=", clientFd,
               " username=", networkLogValue(username),
               " reason=", networkLogValue(reasonForDisconnection),
               " pending_bytes=", pendingBytes,
               " peak_pending_bytes=", peakPendingBytes,
               " clients=", clients->size());
}

// Set write interest explicitly when an output queue switches from empty to partially/fully filled
// This way, we don't unnecessarily call EPOLL_MOD when we don't need to
bool setWriteInterest(int epollfd, int clientFd, bool interested) {
    struct epoll_event ev{};
    ev.data.fd = clientFd;
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (interested) {
        ev.events |= EPOLLOUT;
    }

    if (serverMetrics().active) {
        serverMetrics().epollCtlMod++;
    }
    if (epoll_ctl(epollfd, EPOLL_CTL_MOD, clientFd, &ev) != 0) {
        logNetworkError("epoll.modify_failed", errno, clientFd);
        return false;
    }

    logNetwork(NetworkLogLevel::Debug, "epoll.write_interest",
               "fd=", clientFd, " enabled=", interested ? 1 : 0);

    return true;
}

bool queueOutput(int epollfd, ClientConnection *client, const char *data, size_t numBytes) {
    bool queued = queueOutputCommon(client, data, numBytes, [&](bool interested) {
        return setWriteInterest(epollfd, client->fd, interested);
    });
    if (!queued && errno != 0) {
        logNetworkError("socket.output_failed", errno, client->fd);
    }
    return queued;
}

bool flushOutput(int epollfd, ClientConnection *client) {
    bool flushed = flushOutputCommon(client, [&](bool interested) {
        return setWriteInterest(epollfd, client->fd, interested);
    });
    if (!flushed) {
        logNetworkError("socket.send_failed", errno, client->fd);
    }
    return flushed;
}
