#pragma once

#include <cstddef>
#include <deque>
#include <poll.h>
#include <string>
#include <unordered_map>
#include <vector>

constexpr char PORT[] = "1234";
constexpr int QUEUE_LENGTH = 10;
constexpr int MAX_DATA_SIZE = 256;
constexpr int CLIENT_AWAITING_USERNAME = 0;
constexpr int CLIENT_ACTIVE = 1;

struct ClientConnection {
    int fd;
    std::string username;
    int currRoom;
    char inputBuf[MAX_DATA_SIZE];
    std::deque<std::string> outputQueue;
    size_t outputQueueSize;
    int clientState;
};

struct ChatRoom {
    int roomIdx; // number of the room
    std::string roomName;
    std::vector<int> subscribedClients; // list of all clients that are a member of the chatroom
};

void tokenizeBySpaces(const std::string &input, std::vector<std::string> &tokens);
bool isReservedKeyword(std::string_view cmd);

void signalHandler(int sig);
const char *inet_ntop2(void *addr, char *buf, size_t size);
int getListenerSocket();
void addToPFDs(std::vector<struct pollfd> *pfds, int newfd);
void removePFD(std::vector<struct pollfd> *pfds, int i);
void queueOutput(struct pollfd *pfd, ClientConnection *client, const char *data, size_t numBytes);
bool flushOutput(struct pollfd *pfd, ClientConnection *client);

ClientConnection packClientStruct(int fd, std::string username);
void handleConnection(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients);
void handleClients(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms);
void processExistingConnections(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms);

void processCommand(std::string command);

void createChatRoom(std::string roomName, std::vector<struct ChatRoom> *listOfRooms);
void joinChatRoom(struct ClientConnection *client, std::string roomToJoin, std::vector<ChatRoom> &chatRooms);
void listChatRooms(struct pollfd *pfd, ClientConnection *client, const std::vector<ChatRoom> &chatRooms);
void checkDeleteChatRoom(int roomIdToDelete, std::vector<ChatRoom> &chatRooms);
