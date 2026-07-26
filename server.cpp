#include <asm-generic/socket.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <cctype>
#include <csignal>
#include <poll.h>

#define PORT "1234"
#define QUEUE_LENGTH 10
#define MAX_DATA_SIZE 100

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
    int yes=1;
    int rv;

    struct addrinfo hints, *ai, *p;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if((rv = getaddrinfo(NULL, PORT, &hints, &ai)) != 0) {
        std::cerr << "pollserver: " << gai_strerror(rv) << std::endl;
        exit(1);
    }

    for (p = ai; p != NULL; p = p->ai_next) {
        listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);

        if(listener < 0) {
            continue;
        }

        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        if(bind(listener, p->ai_addr, p->ai_addrlen)) {
            close(listener);
            continue;
        }

        break;
    }

    if (p == NULL) {
        return -1;
    }

    freeaddrinfo(ai);

    if(listen(listener, 10) == -1) {
        return -1;
    }

    return listener;
}

void addToPFDs(struct pollfd **pfds, int newfd, int *fdCount, int *fdSize) {
    if (*fdCount == *fdSize) {
        *fdSize *= 2;
        *pfds = (struct pollfd *)realloc(*pfds, sizeof(**pfds) * (*fdSize));
    }

    (*pfds)[*fdCount].fd = newfd;
    (*pfds)[*fdCount].events = POLLIN;
    (*pfds)[*fdCount].revents = 0;

    (*fdCount)++;
}

void removePFD(struct pollfd pfds[], int i, int *fdCount) {
    pfds[i] = pfds[*fdCount - 1];
    (*fdCount)--;
}

void handleConnection(int listener, int *fdCount, int *fdSize, struct pollfd **pfds) {
    struct sockaddr_storage incomingAddr;
    socklen_t addrlen;
    int incomingfd;
    char remoteIP[INET6_ADDRSTRLEN];

    addrlen = sizeof remoteIP;
    incomingfd = accept(listener, (struct sockaddr *)&incomingAddr, &addrlen);

    if (incomingfd == -1) {
        perror("accept");
    } else {
        addToPFDs(pfds, incomingfd, fdCount, fdSize);

        std::cout << "pollserver: new connection from " << inet_ntop2(&incomingAddr, remoteIP, sizeof remoteIP) << " on socket " << incomingfd << std::endl;
    }
}

void handleClients(int listener, int *fdCount, struct pollfd *pfds, int *pfd_i) {
    char buf[256];
    int numBytes = recv(pfds[*pfd_i].fd, buf, sizeof buf, 0);

    int senderfd = pfds[*pfd_i].fd;

    if (numBytes <= 0) {
        if (numBytes == 0) {
            std::cout << "pollserver: socket " << senderfd << " hung up" << std::endl;
        } else {
            perror("recv");
        }

        close(pfds[*pfd_i].fd);
        removePFD(pfds, *pfd_i, fdCount);

        (*pfd_i)--;
    } else {
        std::cout << "pollserver: recv from fd " << senderfd << ": " << buf;

        for (int j = 0; j < *fdCount; j++) {
            int destfd = pfds[j].fd;

            if (destfd != listener && destfd != senderfd) {
                if (send(destfd, buf, numBytes, 0) == -1) {
                    perror("send");
                }
            }
        }
    }
}

void processExistingConnections(int listener, int *fdCount, int *fdSize, struct pollfd **pfds) {
    for (int i = 0; i < *fdCount; i++) {
        if ((*pfds)[i].revents & (POLLIN | POLLHUP)) {
            if ((*pfds)[i].fd == listener) {
                handleConnection(listener, fdCount, fdSize, pfds);
            } else {
                handleClients(listener, fdCount,  *pfds, &i);
            }

        }
    }
}

int main(void) {
    int listener;

    int fdSize = 5; // starting off with room for 5 connections;
    int fdCount = 0;
    // TODO: SWITCH TO NEW INSTEAD OF MALLOC
    struct pollfd *pfds = (pollfd *)malloc(sizeof *pfds * fdSize);

    // generate a listener and add that as the first pollfd entry
    listener = getListenerSocket();

    if (listener == -1) {
        std::cerr << "error getting listening socket" << std::endl;
        exit(1);
    }

    pfds[0].fd = listener;
    pfds[0].events = POLLIN;

    fdCount = 1;

    puts("pollserver: waiting for connections...");

    while (true) {
        int pollCount = poll(pfds, fdCount, -1);

        if (pollCount == -1) {
            perror("poll");
            exit(1);
        }

        processExistingConnections(listener, &fdCount, &fdSize, &pfds);
    }

    free(pfds); // free once done
}
