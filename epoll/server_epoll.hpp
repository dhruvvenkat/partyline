#pragma once

#include <cstddef>
#include <deque>
#include <poll.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <map>

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

void tokenizeBySpaces(const std::string &input, std::vector<std::string> &tokens);
bool isReservedKeyword(std::string_view cmd);

void signalHandler(int sig);
const char *inet_ntop2(void *addr, char *buf, size_t size);
int getListenerSocket();
void addToPFDs(std::vector<struct pollfd> *pfds, int newfd, std::map<int, int> *pfdMappings);
void removePFD(std::vector<struct pollfd> *pfds, int i, std::map<int, int> *pfdMappings);
void disconnectClient(int epollfd, std::unordered_map<int, ClientConnection> *clients, int clientFd, const std::string &reasonForDisconnection);
bool queueOutput(struct epoll_event *ev, int epollfd, ClientConnection *client, const char *data, size_t numBytes);
bool flushOutput(struct pollfd *pfd, ClientConnection *client);

ClientConnection packClientStruct(int fd, std::string username);
void handleConnection(int listener, int epollfd, std::unordered_map<int, ClientConnection> *clients);
void handleClients(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms);
void processExistingConnections(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms, std::map<int, int> *pfdMappings);

void processCommand(std::string command);

void createChatRoom(std::string roomName, std::vector<struct ChatRoom> *listOfRooms);
void joinChatRoom(struct ClientConnection *client, std::string roomToJoin, std::vector<ChatRoom> &chatRooms);
bool listChatRooms(struct pollfd *pfd, ClientConnection *client, const std::vector<ChatRoom> &chatRooms);
void checkDeleteChatRoom(int roomIdToDelete, std::vector<ChatRoom> &chatRooms);
