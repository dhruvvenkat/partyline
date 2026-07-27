#include "server.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

void signalHandler(int sig) {
    if (sig == SIGINT) {
        std::cout << "\npollserver interrupted, shutting down..." << std::endl;
        exit(sig);
    }

    std::cout << "Interrupt handle " << sig << std::endl;
    exit(sig);
}

ClientConnection packClientStruct(int fd, std::string username) {
    ClientConnection newConnection{};
    newConnection.fd = fd;
    newConnection.username = username;
    newConnection.currRoom = 0;
    newConnection.outputQueueSize = 0;
    newConnection.clientState = username.empty() ? CLIENT_AWAITING_USERNAME : CLIENT_ACTIVE;

    return newConnection;
}

void handleConnection(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients) {
    struct sockaddr_storage incomingAddr;
    socklen_t addrlen = sizeof incomingAddr;
    int incomingfd = accept(listener, (struct sockaddr *)&incomingAddr, &addrlen);
    char remoteIP[INET6_ADDRSTRLEN];

    if (incomingfd == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("accept");
        }
        return;
    }

    if (fcntl(incomingfd, F_SETFL, O_NONBLOCK) == -1) {
        perror("fcntl");
        close(incomingfd);
        return;
    }

    if (send(incomingfd, "enter your username: ", 21, MSG_NOSIGNAL) == -1) {
        perror("server: username prompt did not work");
        close(incomingfd);
        return;
    }

    addToPFDs(pfds, incomingfd);
    clients->insert({incomingfd, packClientStruct(incomingfd, "")});

    std::cout << "pollserver: new connection from " << inet_ntop2(&incomingAddr, remoteIP, sizeof remoteIP) << " on socket " << incomingfd << "; awaiting username" << std::endl;
}

void handleClients(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, ClientConnection> *clients) {
    int senderfd = (*pfds)[*pfd_i].fd;

    ClientConnection *clientSender;
    try {
        clientSender = &clients->at(senderfd);
    } catch (const std::out_of_range&) {
        std::cerr << "server: sending client not found" << std::endl;
        return;
    }

    int numBytes = recv(senderfd, clientSender->inputBuf, sizeof clientSender->inputBuf, 0);

    if (clientSender->clientState == CLIENT_AWAITING_USERNAME) {
        if (numBytes > 0) {
            std::string username(clientSender->inputBuf, numBytes);
            while (!username.empty() && (username.back() == '\n' || username.back() == '\r')) {
                username.pop_back();
            }

            clientSender->username = username;
            clientSender->clientState = CLIENT_ACTIVE;

            std::cout << "pollserver: user " << username << " connected on socket " << senderfd << std::endl;
            std::cout << "\n+------+----------------+\n";
            std::cout << "| FD   | Username       |\n";
            std::cout << "+------+----------------+\n";

            for (const auto& [fd, client] : *clients) {
                std::cout << "| " << fd << "\t| " << client.username << '\n';
            }

            std::cout << "+------+----------------+\n";
        } else if (numBytes == 0) {
            close(senderfd);
            removePFD(pfds, *pfd_i);
            clients->erase(senderfd);
            (*pfd_i)--;
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("recv username");
            close(senderfd);
            removePFD(pfds, *pfd_i);
            clients->erase(senderfd);
            (*pfd_i)--;
        }

        return;
    }

    if (numBytes == 0) {
        std::cout << "pollserver: user " << clientSender->username << " hung up" << std::endl;
        close(senderfd);
        removePFD(pfds, *pfd_i);
        clients->erase(senderfd);
        (*pfd_i)--;
    } else if (numBytes > 0) {
        std::cout << "> pollserver: recv from fd " << senderfd << ": ";
        std::cout.write(clientSender->inputBuf, numBytes);
        std::cout << std::endl;

        for (int j = 0; j < (int)pfds->size(); j++) {
            int destfd = (*pfds)[j].fd;

            if (destfd == listener || destfd == senderfd) {
                continue;
            }

            auto destClient = clients->find(destfd);
            if (destClient != clients->end() && destClient->second.clientState == CLIENT_ACTIVE) {
                queueOutput(&(*pfds)[j], &destClient->second, clientSender->inputBuf, numBytes);
            }
        }
    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
        perror("recv");
        close(senderfd);
        removePFD(pfds, *pfd_i);
        clients->erase(senderfd);
        (*pfd_i)--;
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

    int listener = getListenerSocket();
    if (listener == -1) {
        std::cerr << "error getting listening socket" << std::endl;
        exit(1);
    }

    std::unordered_map<int, ClientConnection> clients;
    std::vector<struct pollfd> pfds{{listener, POLLIN, 0}};

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
