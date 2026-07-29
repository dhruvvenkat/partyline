#include "server.hpp"

#include <arpa/inet.h>
#include <algorithm>
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
#include <sstream>
#include <unordered_set>
#include <cstring>

static const std::unordered_set<std::string> reservedKeywords = {"LIST", "JOIN", "LEAVE", "MSG", "QUIT", "WHERE"};

bool isReservedKeyword(std::string_view cmd) {
    return reservedKeywords.find(std::string(cmd)) != reservedKeywords.end();
}

void tokenizeBySpaces(const std::string &input, std::vector<std::string> &tokens) {
	std::istringstream iss(input);
	std::string buf;

	while (getline(iss, buf, ' ')) {
		tokens.push_back(buf);
	}

	return;
}

static void removeClientFromRooms(int clientFd, std::vector<ChatRoom> &chatRooms) {
    for (auto &room : chatRooms) {
        room.subscribedClients.erase(
            std::remove(room.subscribedClients.begin(), room.subscribedClients.end(), clientFd),
            room.subscribedClients.end());
    }
}

static std::string roomNameForId(int roomId, const std::vector<ChatRoom> &chatRooms) {
    for (const auto &room : chatRooms) {
        if (room.roomIdx == roomId) {
            return room.roomName;
        }
    }
    return {};
}

static void notifyRoomMembers(int roomId, int excludedFd, const std::string &message, int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients) {
    for (int i = 0; i < (int)pfds->size(); i++) {
        int destfd = (*pfds)[i].fd;
        if (destfd == listener || destfd == excludedFd) {
            continue;
        }

        auto destClient = clients->find(destfd);
        if (destClient == clients->end() || destClient->second.clientState != CLIENT_ACTIVE || destClient->second.currRoom != roomId) {
            continue;
        }

        if(queueOutput(&(*pfds)[i], &destClient->second, message.data(), message.size()) == false) {
            disconnectClient(pfds, &i, clients);
        }
    }
}

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

static bool processCommandFrame(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms, ClientConnection *clientSender, const std::string &command) {
    int senderfd = clientSender->fd;
    if (command.empty()) {
        return true;
    }

    std::vector<std::string> tokens;
    tokenizeBySpaces(command, tokens);
    if (!tokens.empty() && isReservedKeyword(tokens[0])) {
        if (tokens[0] == "LIST") {
            std::cout << "list picked" << std::endl;
            if (!listChatRooms(&(*pfds)[*pfd_i], clientSender, *chatRooms)) {
                disconnectClient(pfds, pfd_i, clients);
                return false;
            }
            return true;
        } else if (tokens[0] == "JOIN" && tokens.size() == 2) {
            std::cout << "join picked" << std::endl;
            int oldRoomIdx = clientSender->currRoom;
            std::string oldRoomName = roomNameForId(oldRoomIdx, *chatRooms);
            if (oldRoomName != tokens[1]) {
                std::string leftMessage = "> server: " + clientSender->username + " left room " + oldRoomName + "\n";
                notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, listener, pfds, clients);
            }
            joinChatRoom(clientSender, tokens[1], *chatRooms);
            if (oldRoomName != tokens[1]) {
                std::string joinedMessage = "> server: " + clientSender->username + " joined room " + tokens[1] + "\n";
                notifyRoomMembers(clientSender->currRoom, -1, joinedMessage, listener, pfds, clients);
            }
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);
        } else if (tokens[0] == "LEAVE" && clientSender->currRoom != 0) {
            std::cout << "leave picked" << std::endl;
            int oldRoomIdx = clientSender->currRoom;
            std::string oldRoomName = roomNameForId(oldRoomIdx, *chatRooms);
            std::string leftMessage = "> server: " + clientSender->username + " left room " + oldRoomName + "\n";
            notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, listener, pfds, clients);
            joinChatRoom(clientSender, "main-room", *chatRooms);
            std::string joinedMessage = "> server: " + clientSender->username + " joined room main-room\n";
            notifyRoomMembers(clientSender->currRoom, -1, joinedMessage, listener, pfds, clients);
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);
        } else if (tokens[0] == "WHERE" && tokens.size() == 1) {
            auto userRoom = std::find_if(chatRooms->begin(), chatRooms->end(), [&](const ChatRoom &room) {
                return room.roomIdx == clientSender->currRoom;
            });

            std::string output = userRoom == chatRooms->end()
                ? "> server: room not found\n"
                : "> server: you are in room " + userRoom->roomName + "\n";
            if (!queueOutput(&(*pfds)[*pfd_i], clientSender, output.data(), output.size())) {
                disconnectClient(pfds, pfd_i, clients);
                return false;
            }
        } else if (tokens[0] == "QUIT") {
            int oldRoomIdx = clientSender->currRoom;
            std::string leftMessage = "> server: " + clientSender->username + " has disconnected\n";
            notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, listener, pfds, clients);
            removeClientFromRooms(senderfd, *chatRooms);
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);
            disconnectClient(pfds, pfd_i, clients);
            return false;
        }
    } else {
        std::string formattedOutboundMsg = "> " + clientSender->username + ": ";
        for (const auto &token : tokens) {
            formattedOutboundMsg += token + " ";
        }
        formattedOutboundMsg += "\n";

        std::cout << formattedOutboundMsg;
        for (int j = 0; j < (int)pfds->size(); j++) {
            int destfd = (*pfds)[j].fd;
            if (destfd == listener || destfd == senderfd) {
                continue;
            }

            auto destClient = clients->find(destfd);
            if (destClient == clients->end() || destClient->second.clientState != CLIENT_ACTIVE || destClient->second.currRoom != clientSender->currRoom) {
                continue;
            }

            if (!queueOutput(&(*pfds)[j], &destClient->second, formattedOutboundMsg.data(), formattedOutboundMsg.size())) {
                disconnectClient(pfds, &j, clients);
                return false;
            }
        }
    }

    return true;
}

void handleClients(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms) {
    int senderfd = (*pfds)[*pfd_i].fd;

    ClientConnection *clientSender;
    try {
        clientSender = &clients->at(senderfd);
    } catch (const std::out_of_range&) {
        std::cerr << "server: sending client not found" << std::endl;
        return;
    }

    char recvBuffer[MAX_DATA_SIZE];
    size_t bytesRead = 0;

    while (bytesRead < CLIENT_LOOP_BYTE_READ_BUDGET) {
        ssize_t numBytes = recv(senderfd, recvBuffer, sizeof recvBuffer, 0);

        if (numBytes > 0) {
            bytesRead += numBytes;
            clientSender->inputBuffer.append(recvBuffer, numBytes);

            if (clientSender->inputBuffer.size() > MAX_INPUT_BUFFER_BYTES) {
                std::cerr << "server: input buffer limit exceeded for fd " << senderfd << std::endl;
                disconnectClient(pfds, pfd_i, clients);
                return;
            }

            while (true) {
                size_t newline = clientSender->inputBuffer.find('\n');
                if (newline == std::string::npos) {
                    if (clientSender->inputBuffer.size() > MAX_FRAME_BYTES) {
                        std::cerr << "server: frame limit exceeded for fd " << senderfd << std::endl;
                        disconnectClient(pfds, pfd_i, clients);
                        return;
                    }
                    break;
                }

                if (newline > MAX_FRAME_BYTES) {
                    std::cerr << "server: frame limit exceeded for fd " << senderfd << std::endl;
                    disconnectClient(pfds, pfd_i, clients);
                    return;
                }

                std::string frame = clientSender->inputBuffer.substr(0, newline);
                clientSender->inputBuffer.erase(0, newline + 1);
                if (!frame.empty() && frame.back() == '\r') {
                    frame.pop_back();
                }

                if (clientSender->clientState == CLIENT_AWAITING_USERNAME) {
                    clientSender->username = frame;
                    clientSender->clientState = CLIENT_ACTIVE;
                    joinChatRoom(clientSender, "main-room", *chatRooms);
                    std::string joinedMessage = "> server: " + clientSender->username + " joined room main-room\n";
                    notifyRoomMembers(clientSender->currRoom, -1, joinedMessage, listener, pfds, clients);

                    std::cout << "pollserver: user " << clientSender->username << " connected on socket " << senderfd << std::endl;
                    continue;
                }

                if (!processCommandFrame(listener, pfds, pfd_i, clients, chatRooms, clientSender, frame)) {
                    return;
                }
            }
            continue;
        }

        if (numBytes == 0) {
            std::cout << "pollserver: user " << clientSender->username << " hung up" << std::endl;
            int oldRoomIdx = clientSender->currRoom;
            std::string leftMessage = "> server: " + clientSender->username + " disconnected\n";
            notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, listener, pfds, clients);
            removeClientFromRooms(senderfd, *chatRooms);
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);
            disconnectClient(pfds, pfd_i, clients);
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return;
        }

        perror("recv");
        int oldRoomIdx = clientSender->currRoom;
        std::string leftMessage = "> server: " + clientSender->username + " disconnected\n";
        notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, listener, pfds, clients);
        removeClientFromRooms(senderfd, *chatRooms);
        checkDeleteChatRoom(oldRoomIdx, *chatRooms);
        disconnectClient(pfds, pfd_i, clients);
        return;
    }
}

void processExistingConnections(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms) {
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
            handleClients(listener, pfds, &i, clients, chatRooms);

            if (i < 0 || i >= (int)pfds->size() || (*pfds)[i].fd != fd) {
                continue;
            }
        }

        if (revents & POLLOUT) {
            auto client = clients->find(fd);
            if (client != clients->end() && !flushOutput(&(*pfds)[i], &client->second)) {
                int oldRoomIdx = client->second.currRoom;
                std::string leftMessage = "> server: " + client->second.username + " disconnected\n";
                notifyRoomMembers(oldRoomIdx, fd, leftMessage, listener, pfds, clients);
                removeClientFromRooms(fd, *chatRooms);
                checkDeleteChatRoom(oldRoomIdx, *chatRooms);
                disconnectClient(pfds, &i, clients);
            }
        }
    }
}

void createChatRoom(std::string roomName, std::vector<struct ChatRoom> *listOfRooms) {
    struct ChatRoom newRoom;
    newRoom.roomIdx = 0;
    for (const auto &room : *listOfRooms) {
        newRoom.roomIdx = std::max(newRoom.roomIdx, room.roomIdx + 1);
    }
    newRoom.roomName = roomName;

    listOfRooms->push_back(newRoom);
}

bool listChatRooms(struct pollfd *pfd, ClientConnection *client, const std::vector<ChatRoom> &chatRooms) {
    std::string roomList = "+------+----------------+\n| ID   | Room           |\n+------+----------------+\n";
    for (const auto &room : chatRooms) {
        roomList += "| " + std::to_string(room.roomIdx) + "\t| " + room.roomName + "\n";
    }
    roomList += "+------+----------------+\n";

    return queueOutput(pfd, client, roomList.data(), roomList.size());
}

void joinChatRoom(struct ClientConnection *client, std::string roomToJoin, std::vector<ChatRoom> &chatRooms) {
    ChatRoom *targetRoom = nullptr;
    for (auto &room : chatRooms) {
        if (room.roomName == roomToJoin) {
            targetRoom = &room;
            break;
        }
    }

    if (targetRoom == nullptr) {
        createChatRoom(roomToJoin, &chatRooms);
        targetRoom = &chatRooms.back();
    }

    removeClientFromRooms(client->fd, chatRooms);
    targetRoom->subscribedClients.push_back(client->fd);
    client->currRoom = targetRoom->roomIdx;
}

void checkDeleteChatRoom(int roomIdToDelete, std::vector<ChatRoom> &chatRooms) {
    auto room = std::find_if(chatRooms.begin(), chatRooms.end(), [&](const ChatRoom &candidate) {
        return candidate.roomIdx == roomIdToDelete;
    });

    if (room != chatRooms.end() && room->roomName != "main-room" && room->subscribedClients.empty()) {
        chatRooms.erase(room);
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
    std::vector<struct ChatRoom> chatRooms;

    createChatRoom("main-room", &chatRooms);
    createChatRoom("test-room", &chatRooms);

    puts("pollserver: waiting for connections...");

    while (true) {
        int pollCount = poll(pfds.data(), pfds.size(), -1);

        if (pollCount == -1) {
            perror("poll");
            exit(1);
        }

        processExistingConnections(listener, &pfds, &clients, &chatRooms);
    }
}
