#include "server_epoll.hpp"

#include <arpa/inet.h>
#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <sstream>
#include <unordered_set>
#include <cstring>
#include <sys/epoll.h>

static const std::unordered_set<std::string> reservedKeywords = {"LIST", "JOIN", "LEAVE", "QUIT", "WHERE"};
static volatile std::sig_atomic_t shutdownSignal = 0;

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

static void notifyRoomMembers(int roomId, int excludedFd, const std::string &message, int epollfd, std::unordered_map<int, ClientConnection> *clients, std::vector<struct ChatRoom> &chatRooms) {
    auto room = std::find_if(chatRooms.begin(), chatRooms.end(), [&](const ChatRoom &candidate) {
        return candidate.roomIdx == roomId;
    });
    if (room == chatRooms.end()) {
        return;
    }

    const auto recipients = room->subscribedClients;
    for (int destfd : recipients) {
        if (destfd == excludedFd) {
            continue;
        }

        auto destClient = clients->find(destfd);
        if (destClient == clients->end() ||
            destClient->second.clientState != CLIENT_ACTIVE ||
            destClient->second.currRoom != roomId) {
            continue;
        }

        if (!queueOutput(epollfd, &destClient->second, message.data(), message.size())) {
            int oldRoomIdx = destClient->second.currRoom;
            removeClientFromRooms(destfd, chatRooms);
            disconnectClient(epollfd, clients, destfd, "slow client: room notification output queue overflow");
            checkDeleteChatRoom(oldRoomIdx, chatRooms);
        }
    }
}

void signalHandler(int sig) {
    shutdownSignal = sig;
}

ClientConnection packClientStruct(int fd, std::string username) {
    ClientConnection newConnection{};
    newConnection.fd = fd;
    newConnection.username = username;
    newConnection.currRoom = 0;
    newConnection.outputQueueSize = 0;
    newConnection.peakPendingOutputBytes = 0;
    newConnection.clientState = username.empty() ? CLIENT_AWAITING_USERNAME : CLIENT_ACTIVE;
    return newConnection;
}

// Steps for accepting a new incoming client
void handleConnection(int listener, int epollfd, std::unordered_map<int, ClientConnection> *clients) {
    while (true) {
        struct sockaddr_storage incomingAddr;
        socklen_t addrlen = sizeof incomingAddr;
        int incomingfd = accept(listener, (struct sockaddr *)&incomingAddr, &addrlen);
        char remoteIP[INET6_ADDRSTRLEN];

        if (incomingfd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            logNetworkError("socket.accept_failed", errno, listener);
            return;
        }

        // Set incoming client's fd to nonblocking to prevent the event loop from blocking on this socket.
        if (fcntl(incomingfd, F_SETFL, O_NONBLOCK) == -1) {
            int errorNumber = errno;
            logNetworkError("socket.nonblocking_failed", errorNumber, incomingfd);
            close(incomingfd);
            continue;
        }

        struct epoll_event ev{};
        ev.data.fd = incomingfd;
        ev.events = EPOLLIN;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, incomingfd, &ev) == -1) {
            int errorNumber = errno;
            logNetworkError("epoll.add_failed", errorNumber, incomingfd);
            close(incomingfd);
            continue;
        }

        auto client = clients->emplace(incomingfd, packClientStruct(incomingfd, "")).first;
        constexpr char usernamePrompt[] = "enter your username: ";

        if (!queueOutput(epollfd, &client->second, usernamePrompt, sizeof(usernamePrompt) - 1)) {
            disconnectClient(epollfd, clients, incomingfd, "username prompt output queue overflow");
            continue;
        }

        const char *remote = inet_ntop2(&incomingAddr, remoteIP, sizeof remoteIP);
        logNetwork(NetworkLogLevel::Info, "client.accepted",
                   "fd=", incomingfd,
                   " remote=", networkLogValue(remote == nullptr ? "unknown" : remote),
                   " clients=", clients->size());
    }
}

// 1 frame = 1 command delimited with '\n'
static bool processCommandFrame(int listener, int epollfd, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms, ClientConnection *clientSender, const std::string &command) {
    int senderfd = clientSender->fd;
    if (command.empty()) {
        return true;
    }

    std::vector<std::string> tokens;
    tokenizeBySpaces(command, tokens);
    logNetwork(NetworkLogLevel::Debug, "frame.received",
               "fd=", senderfd,
               " bytes=", command.size(),
               " type=", networkLogValue(
                   !tokens.empty() && isReservedKeyword(tokens[0]) ? tokens[0] : "message"));
    if (!tokens.empty() && isReservedKeyword(tokens[0])) {
        // PARSE ONE OF THE RESERVED KEYWORDS
        if (tokens[0] == "LIST") {
            if (!listChatRooms(epollfd, clientSender, *chatRooms)) {
                disconnectClient(epollfd, clients, senderfd, "slow client: LIST response output queue overflow");

                return false;
            }
            return true;

        } else if (tokens[0] == "JOIN" && tokens.size() == 2) {
            int oldRoomIdx = clientSender->currRoom;
            std::string oldRoomName = roomNameForId(oldRoomIdx, *chatRooms);
            if (oldRoomName != tokens[1]) {
                std::string leftMessage = "> server: " + clientSender->username + " left room " + oldRoomName + "\n";
                notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, epollfd, clients, *chatRooms);
            }
            joinChatRoom(clientSender, tokens[1], *chatRooms);
            if (oldRoomName != tokens[1]) {
                std::string joinedMessage = "> server: " + clientSender->username + " joined room " + tokens[1] + "\n";
                notifyRoomMembers(clientSender->currRoom, -1, joinedMessage, epollfd, clients, *chatRooms);
            }
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);

        } else if (tokens[0] == "LEAVE" && clientSender->currRoom != 0) {
            int oldRoomIdx = clientSender->currRoom;
            std::string oldRoomName = roomNameForId(oldRoomIdx, *chatRooms);
            std::string leftMessage = "> server: " + clientSender->username + " left room " + oldRoomName + "\n";
            notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, epollfd, clients, *chatRooms);
            joinChatRoom(clientSender, "main-room", *chatRooms);
            std::string joinedMessage = "> server: " + clientSender->username + " joined room main-room\n";
            notifyRoomMembers(clientSender->currRoom, -1, joinedMessage, epollfd, clients, *chatRooms);
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);

        } else if (tokens[0] == "WHERE" && tokens.size() == 1) {
            auto userRoom = std::find_if(chatRooms->begin(), chatRooms->end(), [&](const ChatRoom &room) {
                return room.roomIdx == clientSender->currRoom;
            });

            std::string output = userRoom == chatRooms->end()
                ? "> server: room not found\n"
                : "> server: you are in room " + userRoom->roomName + "\n";

            if (!queueOutput(epollfd, clientSender, output.data(), output.size())) {
                int oldRoomIdx = clientSender->currRoom;
                removeClientFromRooms(senderfd, *chatRooms);
                disconnectClient(epollfd, clients, senderfd, "slow client: WHERE response output queue overflow");
                checkDeleteChatRoom(oldRoomIdx, *chatRooms);
                return false;
            }

        } else if (tokens[0] == "QUIT") {

            int oldRoomIdx = clientSender->currRoom;
            std::string leftMessage = "> server: " + clientSender->username + " has disconnected\n";
            notifyRoomMembers(oldRoomIdx, senderfd, leftMessage, epollfd, clients, *chatRooms);
            removeClientFromRooms(senderfd, *chatRooms);
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);
            disconnectClient(epollfd, clients, senderfd, "client requested QUIT");
            return false;

        }
    } else {
        std::string formattedOutboundMsg = "> " + clientSender->username + ": ";
        for (const auto &token : tokens) {
            formattedOutboundMsg += token + " ";
        }
        formattedOutboundMsg += "\n";

        // Since room deletion changes the indexes of the rooms themselves, we have to do a sweep before broadcasting to make sure we don't broadcast to the wrong room ID
        auto broadcastRoom = std::find_if(chatRooms->begin(), chatRooms->end(), [&](const ChatRoom &room) {
            return room.roomIdx == clientSender->currRoom;
        });
        if (broadcastRoom == chatRooms->end()) {
            return false;
        }

        size_t recipients = 0;
        for (int destfd : broadcastRoom->subscribedClients) {
            if (destfd == listener || destfd == senderfd) {
                continue;
            }

            auto destClient = clients->find(destfd);
            if (destClient == clients->end() || destClient->second.clientState != CLIENT_ACTIVE || destClient->second.currRoom != clientSender->currRoom) {
                continue;
            }

            if (!queueOutput(epollfd, &destClient->second, formattedOutboundMsg.data(), formattedOutboundMsg.size())) {
                int oldRoomIdx = destClient->second.currRoom;
                removeClientFromRooms(destfd, *chatRooms);
                disconnectClient(epollfd, clients, destfd, "slow client: chat message output queue overflow");
                checkDeleteChatRoom(oldRoomIdx, *chatRooms);
                return false;
                //j--; // Since disconnected client was swapped-and-poppped, we have to review the current slot again since it's populated with a new client
            }
            recipients++;
        }
        logNetwork(NetworkLogLevel::Debug, "chat.broadcast_queued",
                   "fd=", senderfd,
                   " room_id=", clientSender->currRoom,
                   " bytes=", formattedOutboundMsg.size(),
                   " recipients=", recipients);
    }

    return true;
}

// Handle incoming messages from the client specified by the polling loop in handleExistingConnections
void handleClients(int listener, int incomingfd, int epollfd, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms) {

    ClientConnection *clientSender;
    try {
        clientSender = &clients->at(incomingfd);
    } catch (const std::out_of_range&) {
        logNetwork(NetworkLogLevel::Warning, "client.event_stale", "fd=", incomingfd);
        return;
    }

    char recvBuffer[MAX_DATA_SIZE];
    size_t bytesRead = 0;
    size_t framesProcessed = 0;

    while (bytesRead < MAX_BYTES_READ_PER_POLL && framesProcessed < MAX_PROCESSED_FRAMES_PER_POLL) {
        ssize_t numBytes = recv(incomingfd, recvBuffer, sizeof recvBuffer, 0);

        if (numBytes > 0) {
            bytesRead += numBytes;
            clientSender->inputBuffer.append(recvBuffer, numBytes);
            logNetwork(NetworkLogLevel::Debug, "socket.received",
                       "fd=", incomingfd,
                       " bytes=", numBytes,
                       " buffered_bytes=", clientSender->inputBuffer.size());

            if (clientSender->inputBuffer.size() > MAX_INPUT_BUFFER_BYTES) {
                // Slow-client protection
                logNetwork(NetworkLogLevel::Warning, "socket.input_limit_exceeded",
                           "fd=", incomingfd,
                           " buffered_bytes=", clientSender->inputBuffer.size(),
                           " limit_bytes=", MAX_INPUT_BUFFER_BYTES);
                disconnectClient(epollfd, clients, incomingfd, "input buffer limit exceeded");
                return;
            }

            while (true) {
                size_t newline = clientSender->inputBuffer.find('\n');
                if (newline == std::string::npos) {
                    if (clientSender->inputBuffer.size() > MAX_FRAME_BYTES) {
                        logNetwork(NetworkLogLevel::Warning, "frame.limit_exceeded",
                                   "fd=", incomingfd,
                                   " bytes=", clientSender->inputBuffer.size(),
                                   " limit_bytes=", MAX_FRAME_BYTES);
                        disconnectClient(epollfd, clients, incomingfd, "input buffer limit exceeded");
                        return;
                    }
                    break;
                }

                if (newline > MAX_FRAME_BYTES) {
                    logNetwork(NetworkLogLevel::Warning, "frame.limit_exceeded",
                               "fd=", incomingfd,
                               " bytes=", newline,
                               " limit_bytes=", MAX_FRAME_BYTES);
                    disconnectClient(epollfd, clients, incomingfd, "input buffer limit exceeded");
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
                    notifyRoomMembers(clientSender->currRoom, -1, joinedMessage, epollfd, clients, *chatRooms);

                    logNetwork(NetworkLogLevel::Info, "client.authenticated",
                               "fd=", incomingfd,
                               " username=", networkLogValue(clientSender->username),
                               " room_id=", clientSender->currRoom);
                    continue;
                }

                if (!processCommandFrame(listener, epollfd, clients, chatRooms, clientSender, frame)) {
                    return;
                }

                framesProcessed++;
            }
            continue;
        }

        if (numBytes == 0) {
            int oldRoomIdx = clientSender->currRoom;
            std::string leftMessage = "> server: " + clientSender->username + " disconnected\n";
            notifyRoomMembers(oldRoomIdx, incomingfd, leftMessage, epollfd, clients, *chatRooms);
            removeClientFromRooms(incomingfd, *chatRooms);
            checkDeleteChatRoom(oldRoomIdx, *chatRooms);
            disconnectClient(epollfd, clients, incomingfd, "peer closed connection");
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return;
        }

        int errorNumber = errno;
        logNetworkError("socket.receive_failed", errorNumber, incomingfd);
        int oldRoomIdx = clientSender->currRoom;
        std::string leftMessage = "> server: " + clientSender->username + " disconnected\n";
        notifyRoomMembers(oldRoomIdx, incomingfd, leftMessage, epollfd, clients, *chatRooms);
        removeClientFromRooms(incomingfd, *chatRooms);
        checkDeleteChatRoom(oldRoomIdx, *chatRooms);
        disconnectClient(epollfd, clients, incomingfd, "recv failed");

        return;
    }
}

void processExistingConnections(int listener, int epollfd, std::vector<struct epoll_event> &ready, int numReady, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms) {
    for (int i = 0; i < numReady; i++) {
        int fd = ready[i].data.fd;
        uint32_t flags = ready[i].events; // Bitmask representing the state of the epoll_event
        logNetwork(NetworkLogLevel::Debug, "epoll.event",
                   "fd=", fd, " flags=", flags);

        if (fd == listener) {
            // Allow a new incoming connection (generate a new pfd + ClientConnection)
            if (flags & EPOLLIN) {
                handleConnection(listener, epollfd, clients);
            }
            continue;
        }

        if (flags & (EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
            handleClients(listener, fd, epollfd, clients, chatRooms);
        }

        if (i < 0 || i >= numReady || ready[i].data.fd != fd) {
            continue;
        }

        if (flags & EPOLLOUT) {
            auto client = clients->find(fd);
            if (client != clients->end() && !flushOutput(epollfd, &client->second)) {
                int oldRoomIdx = client->second.currRoom;
                std::string leftMessage = "> server: " + client->second.username + " disconnected\n";
                notifyRoomMembers(oldRoomIdx, fd, leftMessage, epollfd, clients, *chatRooms);
                removeClientFromRooms(fd, *chatRooms);
                checkDeleteChatRoom(oldRoomIdx, *chatRooms);
                disconnectClient(epollfd, clients, fd, "send failed");
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

bool listChatRooms(int epollfd, ClientConnection *client, const std::vector<ChatRoom> &chatRooms) {
    std::string roomList = "+------+----------------+\n| ID   | Room           |\n+------+----------------+\n";
    for (const auto &room : chatRooms) {
        roomList += "| " + std::to_string(room.roomIdx) + "\t| " + room.roomName + "\n";
    }
    roomList += "+------+----------------+\n";

    return queueOutput(epollfd, client, roomList.data(), roomList.size());
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
    signal(SIGTERM, signalHandler);

    int listener = getListenerSocket();
    if (listener == -1) {
        logNetwork(NetworkLogLevel::Error, "server.listener_failed");
        exit(1);
    }

    int epollfd = epoll_create1(0); // epoll_create1() is the newer version of epoll_create() that ignores the archaic size parameter
    if (epollfd == -1) {
        logNetworkError("epoll.create_failed", errno);
        exit(1);
    }

    struct epoll_event ev;
    ev.data.fd = listener;
    ev.events = EPOLLIN;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, listener, &ev) == -1) {
        logNetworkError("epoll.add_listener_failed", errno, listener);
        exit(1);
    }

    std::unordered_map<int, ClientConnection> clients;
    std::vector<struct ChatRoom> chatRooms;
    std::vector<struct epoll_event> ready(MAX_EVENTS);

    createChatRoom("main-room", &chatRooms);

    logNetwork(NetworkLogLevel::Info, "server.started",
               "listener_fd=", listener,
               " epoll_fd=", epollfd,
               " max_events=", MAX_EVENTS);

    while (shutdownSignal == 0) {
        // Blocking until an event occurs for one of the file descriptors we're polling
        int readyfds = epoll_wait(epollfd, ready.data(), MAX_EVENTS, -1);

        if (readyfds == -1) {
            if (errno == EINTR) {
                continue;
            }
            logNetworkError("epoll.wait_failed", errno, epollfd);
            exit(1);
        }

        logNetwork(NetworkLogLevel::Debug, "epoll.batch",
                   "ready=", readyfds, " clients=", clients.size());

        processExistingConnections(listener, epollfd, ready, readyfds, &clients, &chatRooms);
    }

    logNetwork(NetworkLogLevel::Info, "server.stopped",
               "signal=", shutdownSignal, " clients=", clients.size());
    if (close(listener) == -1) {
        logNetworkError("listener.close_failed", errno, listener);
    }
    if (close(epollfd) == -1) {
        logNetworkError("epoll.close_failed", errno, epollfd);
    }
    return 0;
}
