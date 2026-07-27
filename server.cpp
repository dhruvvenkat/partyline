#include <asm-generic/socket.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <deque>
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
#define MAX_DATA_SIZE 256

struct ClientConnection {
    int fd;
    std::string username;
    int currRoom;
    char inputBuf[MAX_DATA_SIZE];
    std::deque<std::string> outputQueue;
    size_t outputQueueSize;
    int clientState;
};

void signalHandler(int sig) {
    if (sig == 2) {
        std::cout << "\npollserver interrupted, shutting down..." << std::endl;
        exit(sig);
    }

    std::cout << "Interrupt handle " << sig << std::endl;
    exit(sig);
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

void addToPFDs(std::vector<struct pollfd> *pfds, int newfd) {

    struct pollfd newpfd;
    newpfd.fd = newfd;
    newpfd.events = POLLIN;
    newpfd.revents = 0;

    pfds->push_back(newpfd);

}

struct ClientConnection packClientStruct(int fd, std::string username) {
    struct ClientConnection newConnection{};
    newConnection.fd = fd;
    newConnection.username = username;
    newConnection.currRoom = 0;
    newConnection.outputQueueSize = 0;
    newConnection.clientState = 0;

    return newConnection;
}

void removePFD(std::vector<struct pollfd> *pfds, int i) {
    (*pfds)[i] = pfds->back();
    pfds->pop_back();
}

void queueOutput(struct pollfd *pfd, ClientConnection *client, const char *data, size_t numBytes) {
    client->outputQueue.emplace_back(data, numBytes);
    client->outputQueueSize += numBytes;
    pfd->events |= POLLOUT;
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

void handleConnection(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients) {
    struct sockaddr_storage incomingAddr;
    socklen_t addrlen;
    int incomingfd;
    char remoteIP[INET6_ADDRSTRLEN];
    int numBytes;

    char newClientUsername[MAX_DATA_SIZE];

    addrlen = sizeof incomingAddr;
    incomingfd = accept(listener, (struct sockaddr *)&incomingAddr, &addrlen);

    if (incomingfd == -1) {
        perror("accept");
    } else {
        if (send(incomingfd, "enter your username: ", 22, 0) == -1) {
            perror("server: username prompt did not work");
        }

        if ((numBytes = recv(incomingfd, newClientUsername, sizeof newClientUsername, 0)) <= 0) {
            if (numBytes == -1) {
                perror("server: username not receieved");
            }
            close(incomingfd);
            return;
        }

        std::string username(newClientUsername, numBytes);
        while (!username.empty() && (username.back() == '\n' || username.back() == '\r')) {
            username.pop_back();
        }
        // for (const auto &[desc, client] : clients) {
        //     if (client.username == newClientUsername) {
        //         std::cerr << "server: user already exists";
        //     }
        // }

        addToPFDs(pfds, incomingfd);
        struct ClientConnection newClient = packClientStruct(incomingfd, username);


        clients->insert({incomingfd, newClient});

        std::cout << "pollserver: new connection from " << inet_ntop2(&incomingAddr, remoteIP, sizeof remoteIP) << " on socket " << incomingfd << " with username " << username << std::endl;

        std::cout << "\n+------+----------------+\n";
        std::cout << "| FD   | Username       |\n";
        std::cout << "+------+----------------+\n";

        for (const auto& [fd, client] : *clients) {
            std::cout << "| " << fd << "\t| " << client.username << '\n';
        }

        std::cout << "+------+----------------+\n";
    }
}

void handleClients(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, struct ClientConnection> *clients) {
    int senderfd = (*pfds)[*pfd_i].fd;

    struct ClientConnection *clientSender;
    try {
        clientSender = &clients->at(senderfd);
    } catch (std::out_of_range) {
        std::cerr << "server: sending client not found" << std::endl;
        return;
    }

    int numBytes = recv(senderfd, clientSender->inputBuf, sizeof clientSender->inputBuf, 0);

    if (numBytes <= 0) {
        if (numBytes == 0) {
            std::cout << "pollserver: user " << clientSender->username << " hung up" << std::endl;
        } else {
            perror("recv");
        }

        close((*pfds)[*pfd_i].fd);
        removePFD(pfds, *pfd_i);
        clients->erase(senderfd);

        (*pfd_i)--;
    } else {
        std::cout << "> pollserver: recv from fd " << senderfd << ": ";
        std::cout.write(clientSender->inputBuf, numBytes);
        std::cout << std::endl;

        for (int j = 0; j < (int)pfds->size(); j++) {
            int destfd = (*pfds)[j].fd;

            if (destfd != listener && destfd != senderfd) {
                auto destClient = clients->find(destfd);
                if (destClient != clients->end()) {
                    queueOutput(&(*pfds)[j], &destClient->second, clientSender->inputBuf, numBytes);
                }
            }
        }
    }
}

void processExistingConnections(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients) {
    for (int i = 0; i < (int)pfds->size(); i++) {
        short revents = (*pfds)[i].revents;
        int fd = (*pfds)[i].fd;

        if (fd == listener) {
            if (revents & POLLIN) {
                handleConnection(listener, pfds, clients);
            }
            continue;
        }

        if (revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) {
            handleClients(listener, pfds, &i, clients);

            if (i < 0 || i >= (int)pfds->size() || (*pfds)[i].fd != fd) {
                continue;
            }
        }

        if (revents & POLLOUT) {
            auto client = clients->find(fd);
            if (client != clients->end() && !flushOutput(&(*pfds)[i], &client->second)) {
                close(fd);
                removePFD(pfds, i);
                clients->erase(fd);
                i--;
            }
        }
    }
}

int main(void) {
    signal(SIGINT, signalHandler);

    int listener;

    std::unordered_map<int, struct ClientConnection> clients;
    std::vector<struct pollfd> pfds;

    // generate a listener and add that as the first pollfd entry
    listener = getListenerSocket();
    pfds.push_back({listener, POLLIN, 0});

    if (listener == -1) {
        std::cerr << "error getting listening socket" << std::endl;
        exit(1);
    }

    puts("pollserver: waiting for connections...");

    while (true) {
        int pollCount = poll(pfds.data(), pfds.size(), -1);

        if (pollCount == -1) {
            perror("poll");
            exit(1);
        }

        processExistingConnections(listener, &pfds, &clients);
    }
}
