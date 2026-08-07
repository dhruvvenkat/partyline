#pragma once

#include <cstddef>
#include <deque>
#include <poll.h>
#include <string>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>
#include <map>
#include <sstream>
#include <string_view>
#include <utility>

constexpr char PORT[] = "1234";
constexpr int QUEUE_LENGTH = 10;
constexpr int MAX_DATA_SIZE = 256;
constexpr size_t MAX_FRAME_BYTES = 4096;
constexpr size_t MAX_INPUT_BUFFER_BYTES = 8192;
constexpr int CLIENT_AWAITING_USERNAME = 0;
constexpr int CLIENT_ACTIVE = 1;
constexpr size_t MAX_PENDING_OUTPUT_BYTES = 16384; // 16kb of pending output space for slow-client protection/backpressure
constexpr size_t MAX_BYTES_READ_PER_POLL = 8192;
constexpr size_t MAX_BYTES_WRITTEN_PER_POLL = 8192;
constexpr size_t MAX_PROCESSED_FRAMES_PER_POLL = 32;
constexpr int MAX_EVENTS = 64;

struct PendingWrite {
    std::string data;
    size_t offset = 0;
};

struct ClientConnection {
    int fd;
    std::string username;
    int currRoom;
    std::string inputBuffer;
    std::deque<PendingWrite> outputQueue;
    size_t outputQueueSize;
    size_t peakPendingOutputBytes;
    int clientState;
};

struct ChatRoom {
    int roomIdx; // number of the room
    std::string roomName;
    std::vector<int> subscribedClients; // list of all clients that are a member of the chatroom as file descriptors
};

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
