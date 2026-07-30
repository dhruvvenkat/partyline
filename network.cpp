#include "server.hpp"

#include <arpa/inet.h>
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

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

    if ((rv = getaddrinfo(NULL, PORT, &hints, &ai)) != 0) {
        std::cerr << "pollserver: " << gai_strerror(rv) << std::endl;
        exit(1);
    }

    for (p = ai; p != NULL; p = p->ai_next) {
        listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);

        if (listener < 0) {
            continue;
        }

        if (fcntl(listener, F_SETFL, O_NONBLOCK) == -1) {
            close(listener);
            continue;
        }

        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        if (bind(listener, p->ai_addr, p->ai_addrlen)) {
            close(listener);
            continue;
        }

        break;
    }

    if (p == NULL) {
        return -1;
    }

    freeaddrinfo(ai);

    if (listen(listener, QUEUE_LENGTH) == -1) {
        close(listener);
        return -1;
    }

    return listener;
}

void addToPFDs(std::vector<struct pollfd> *pfds, int newfd) {
    pfds->push_back({newfd, POLLIN, 0});
}

// Swap-and-pop to remove the relevant PFD
void removePFD(std::vector<struct pollfd> *pfds, int i) {
    (*pfds)[i] = pfds->back();
    pfds->pop_back();
}

void disconnectClient(std::vector<struct pollfd> *pfds, int *currentPfdIndex, std::unordered_map<int, ClientConnection> *clients, int clientFd, const std::string &reasonForDisconnection) {
    auto pfd = std::find_if(pfds->begin(), pfds->end(), [clientFd](const struct pollfd &candidate) {
        return candidate.fd == clientFd;
    });
    if (pfd == pfds->end()) {
        return;
    }

    int removedIndex = static_cast<int>(pfd - pfds->begin());
    size_t pendingBytes = 0;
    size_t peakPendingBytes = 0;
    auto client = clients->find(clientFd);
    if (client != clients->end()) {
        pendingBytes = client->second.outputQueueSize;
        peakPendingBytes = client->second.peakPendingOutputBytes;
    }

    close(clientFd);
    removePFD(pfds, removedIndex);
    clients->erase(clientFd);
    if (currentPfdIndex != nullptr && removedIndex <= *currentPfdIndex) {
        (*currentPfdIndex)--;
    }
    std::cout << "disconnect fd=" << clientFd
              << " reason=" << reasonForDisconnection
              << " pending=" << pendingBytes
              << " peak=" << peakPendingBytes << std::endl;
}

bool queueOutput(struct pollfd *pfd, ClientConnection *client, const char *data, size_t numBytes) {
    // Slow-client protection: reject an enqueue that would exceed this client's byte budget
    if (client->outputQueueSize > MAX_PENDING_OUTPUT_BYTES ||
        numBytes > MAX_PENDING_OUTPUT_BYTES - client->outputQueueSize) {
        return false;
    }

    client->outputQueue.push_back({std::string(data, numBytes), 0});
    client->outputQueueSize += numBytes;
    client->peakPendingOutputBytes = std::max(client->peakPendingOutputBytes, client->outputQueueSize);
    pfd->events |= POLLOUT;
    return true;
}

bool flushOutput(struct pollfd *pfd, ClientConnection *client) {
    while (!client->outputQueue.empty()) {
        PendingWrite &pending = client->outputQueue.front();
        ssize_t numBytes = send(client->fd, pending.data.data() + pending.offset, pending.data.size() - pending.offset, MSG_NOSIGNAL);

        if (numBytes > 0) {
            pending.offset += numBytes;
            client->outputQueueSize -= numBytes;

            if (pending.offset == pending.data.size()) {
                client->outputQueue.pop_front();
            }
        } else if (numBytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            perror("send");
            return false;
        }
    }

    if (client->outputQueue.empty()) {
        pfd->events &= ~POLLOUT;
    }

    return true;
}
