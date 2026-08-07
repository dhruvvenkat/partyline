#pragma once

#include "../common/server_common.hpp"

#include <poll.h>
#include <unordered_map>
#include <vector>
#include <map>

void tokenizeBySpaces(const std::string &input, std::vector<std::string> &tokens);
bool isReservedKeyword(std::string_view cmd);

void signalHandler(int sig);
const char *inet_ntop2(void *addr, char *buf, size_t size);
int getListenerSocket();
void addToPFDs(std::vector<struct pollfd> *pfds, int newfd, std::map<int, int> *pfdMappings);
void removePFD(std::vector<struct pollfd> *pfds, int i, std::map<int, int> *pfdMappings);
void disconnectClient(std::vector<struct pollfd> *pfds, int *currentPfdIndex, std::unordered_map<int, ClientConnection> *clients, int clientFd, const std::string &reasonForDisconnection,  std::map<int, int> *pfdMappings);
bool queueOutput(struct pollfd *pfd, ClientConnection *client, const char *data, size_t numBytes);
bool flushOutput(struct pollfd *pfd, ClientConnection *client);

ClientConnection packClientStruct(int fd, std::string username);
void handleConnection(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients,  std::map<int, int> *pfdMappings);
void handleClients(int listener, std::vector<struct pollfd> *pfds, int *pfd_i, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms);
void processExistingConnections(int listener, std::vector<struct pollfd> *pfds, std::unordered_map<int, ClientConnection> *clients, std::vector<ChatRoom> *chatRooms, std::map<int, int> *pfdMappings);

void processCommand(std::string command);

void createChatRoom(std::string roomName, std::vector<struct ChatRoom> *listOfRooms);
void joinChatRoom(struct ClientConnection *client, std::string roomToJoin, std::vector<ChatRoom> &chatRooms);
bool listChatRooms(struct pollfd *pfd, ClientConnection *client, const std::vector<ChatRoom> &chatRooms);
void checkDeleteChatRoom(int roomIdToDelete, std::vector<ChatRoom> &chatRooms);
