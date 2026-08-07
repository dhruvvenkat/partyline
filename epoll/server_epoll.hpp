#pragma once

#include "../common/server_common.hpp"

#include <poll.h>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>
#include <map>
#include <sstream>
#include <string_view>
#include <utility>

constexpr int MAX_EVENTS = 4096;

enum class NetworkLogLevel { Debug, Info, Warning, Error, Off };

bool networkLogEnabled(NetworkLogLevel level);
void writeNetworkLog(NetworkLogLevel level, std::string_view event, const std::string &details);
std::string networkLogValue(std::string_view value);
void logNetworkError(std::string_view event, int errorNumber, int fd = -1);

template <typename... Values>
void logNetwork(NetworkLogLevel level, std::string_view event, Values &&...values) {
    if (!networkLogEnabled(level)) {
        return;
    }

    std::ostringstream details;
    ((details << std::forward<Values>(values)), ...);
    writeNetworkLog(level, event, details.str());
}

void tokenizeBySpaces(const std::string &input, std::vector<std::string> &tokens);
bool isReservedKeyword(std::string_view cmd);

void signalHandler(int sig);
const char *inet_ntop2(void *addr, char *buf, size_t size);
int getListenerSocket();
void addToPFDs(std::vector<struct pollfd> *pfds, int newfd, std::map<int, int> *pfdMappings);
void removePFD(std::vector<struct pollfd> *pfds, int i, std::map<int, int> *pfdMappings);
void disconnectClient(int epollfd, std::unordered_map<int, ClientConnection> *clients, int clientFd, const std::string &reasonForDisconnection);
bool setWriteInterest(int epollfd, int clientFd, bool interested);
bool queueOutput(int epollfd, ClientConnection *client, const char *data, size_t numBytes);
bool flushOutput(int epollfd, ClientConnection *client);

ClientConnection packClientStruct(int fd, std::string username);
void handleConnection(int listener, int epollfd, std::unordered_map<int, ClientConnection> *clients);
void handleClients(int listener, int incomingfd, int epollfd, std::vector<struct epoll_event> &ready, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms);
void processExistingConnections(int listener, int epollfd, std::vector<struct epoll_event> &ready, int numReady, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms);

void processCommand(std::string command);

void createChatRoom(std::string roomName, std::vector<struct ChatRoom> *listOfRooms);
void joinChatRoom(struct ClientConnection *client, std::string roomToJoin, std::vector<ChatRoom> &chatRooms);
bool listChatRooms(int epollfd, ClientConnection *client, const std::vector<ChatRoom> &chatRooms);
void checkDeleteChatRoom(int roomIdToDelete, std::vector<ChatRoom> &chatRooms);
