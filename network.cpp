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
    close(clientFd);
    removePFD(pfds, removedIndex);
    clients->erase(clientFd);
    if (currentPfdIndex != nullptr && removedIndex <= *currentPfdIndex) {
        (*currentPfdIndex)--;
    }
    std::cout << "client disconnected: " << reasonForDisconnection << std::endl;
}

bool queueOutput(struct pollfd *pfd, ClientConnection *client, const char *data, size_t numBytes) {
    // Slow-client protection; if a client's output queue exceeds 512 bytes, the connection is stalled and we cut the connection
    if ((client->outputQueueSize + numBytes) < OUTPUT_QUEUE_MAX) {
        client->outputQueue.emplace_back(data, numBytes);
        client->outputQueueSize += numBytes;
        pfd->events |= POLLOUT;

        return true;
    }

    return false;
}

bool flushOutput(struct pollfd *pfd, ClientConnection *client) {
    while (!client->outputQueue.empty()) {
        std::string &pending = client->outputQueue.front();
        ssize_t numBytes = send(client->fd, pending.data(), pending.size(), MSG_NOSIGNAL);

        if (numBytes > 0) {
            pending.erase(0, numBytes);
            client->outputQueueSize -= numBytes;

            if (pending.empty()) {
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
