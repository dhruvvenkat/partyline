#include "chat_server.hpp"
#include "server_common.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <iterator>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace {
enum class LogLevel { Debug, Info, Warning, Error, Off };
volatile std::sig_atomic_t shutdownSignal = 0;

LogLevel configuredLogLevel() {
    static const LogLevel level = [] {
        const char *configured = std::getenv("CHAT_LOG_LEVEL");
        if (configured == nullptr) {
            return LogLevel::Info;
        }
        std::string value(configured);
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        if (value == "debug") return LogLevel::Debug;
        if (value == "warn" || value == "warning") return LogLevel::Warning;
        if (value == "error") return LogLevel::Error;
        if (value == "off") return LogLevel::Off;
        return LogLevel::Info;
    }();
    return level;
}

bool logEnabled(LogLevel level) {
    LogLevel configured = configuredLogLevel();
    return configured != LogLevel::Off && level >= configured;
}

std::string logValue(std::string_view value) {
    std::ostringstream escaped;
    escaped << '"';
    for (unsigned char ch : value) {
        switch (ch) {
            case '\\': escaped << "\\\\"; break;
            case '"': escaped << "\\\""; break;
            case '\n': escaped << "\\n"; break;
            case '\r': escaped << "\\r"; break;
            case '\t': escaped << "\\t"; break;
            default:
                if (ch < 0x20 || ch == 0x7f) {
                    escaped << "\\x" << std::hex << std::setw(2) << std::setfill('0')
                            << static_cast<int>(ch) << std::dec;
                } else {
                    escaped << static_cast<char>(ch);
                }
        }
    }
    escaped << '"';
    return escaped.str();
}

template <typename... Values>
void logLine(LogLevel level, std::string_view event, Values &&...values) {
    if (!logEnabled(level)) {
        return;
    }
    static const char *names[] = {"debug", "info", "warn", "error", "off"};
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
    std::tm utc{};
    gmtime_r(&currentTime, &utc);

    std::ostringstream output;
    output << "timestamp=" << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
           << '.' << std::setfill('0') << std::setw(3) << milliseconds.count() << 'Z'
           << " level=" << names[static_cast<int>(level)] << " event=" << event;
    ((output << ' ' << std::forward<Values>(values)), ...);
    output << '\n';
    const std::string line = output.str();
    std::fwrite(line.data(), 1, line.size(), stderr);
}

void logError(std::string_view event, int errorNumber, int fd = -1) {
    if (fd >= 0) {
        logLine(LogLevel::Error, event, "fd=" + std::to_string(fd),
                "errno=" + std::to_string(errorNumber),
                "error=" + logValue(std::strerror(errorNumber)));
    } else {
        logLine(LogLevel::Error, event, "errno=" + std::to_string(errorNumber),
                "error=" + logValue(std::strerror(errorNumber)));
    }
}

void handleShutdown(int signalNumber) {
    shutdownSignal = signalNumber;
}

const char *addressText(sockaddr_storage *address, char *buffer, size_t size) {
    void *source = nullptr;
    if (address->ss_family == AF_INET) {
        source = &reinterpret_cast<sockaddr_in *>(address)->sin_addr;
    } else if (address->ss_family == AF_INET6) {
        source = &reinterpret_cast<sockaddr_in6 *>(address)->sin6_addr;
    } else {
        return nullptr;
    }
    return inet_ntop(address->ss_family, source, buffer, size);
}

int listenerSocket() {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    const char *port = std::getenv("CHAT_SERVER_PORT");
    if (port == nullptr || *port == '\0') {
        port = PORT;
    }

    addrinfo *addresses = nullptr;
    int status = getaddrinfo(nullptr, port, &hints, &addresses);
    if (status != 0) {
        logLine(LogLevel::Error, "listener.resolve_failed",
                "status=" + std::to_string(status), "error=" + logValue(gai_strerror(status)));
        return -1;
    }

    int listener = -1;
    int reuse = 1;
    for (addrinfo *address = addresses; address != nullptr; address = address->ai_next) {
        listener = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (listener == -1) {
            continue;
        }
        if (fcntl(listener, F_SETFL, O_NONBLOCK) == -1 ||
            setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse) == -1 ||
            bind(listener, address->ai_addr, address->ai_addrlen) == -1) {
            close(listener);
            listener = -1;
            continue;
        }
        break;
    }
    freeaddrinfo(addresses);
    if (listener == -1 || listen(listener, QUEUE_LENGTH) == -1) {
        if (listener != -1) {
            close(listener);
        }
        return -1;
    }
    logLine(LogLevel::Info, "listener.ready", "fd=" + std::to_string(listener),
            "port=" + std::string(port), "backlog=" + std::to_string(QUEUE_LENGTH));
    return listener;
}

class ChatServer {
public:
    explicit ChatServer(ReadinessDispatcher &dispatcher)
        : dispatcher(dispatcher) {
        rooms.push_back({0, "main-room", {}});
    }

    int run() {
        listener = listenerSocket();
        if (listener == -1 || !dispatcher.add(listener)) {
            logError("server.listener_failed", errno, listener);
            return 1;
        }
        logLine(LogLevel::Info, "server.started", "backend=" + std::string(dispatcher.name()),
                "listener_fd=" + std::to_string(listener));

        std::vector<ReadyEvent> ready;
        while (shutdownSignal == 0) {
            serviceMetricsSignals();
            int readyCount = dispatcher.wait(ready);
            if (readyCount == -1) {
                if (errno == EINTR) {
                    serviceMetricsSignals();
                    continue;
                }
                logError("dispatcher.wait_failed", errno);
                return 1;
            }
            serverMetrics().recordWait(ready.size(), dispatcher.maxEvents());
            beginServerIteration();
            logLine(LogLevel::Debug, "dispatcher.batch",
                    "backend=" + std::string(dispatcher.name()),
                    "ready=" + std::to_string(ready.size()),
                    "clients=" + std::to_string(clients.size()));
            for (const ReadyEvent &event : ready) {
                handleEvent(event);
            }
        }
        serviceMetricsSignals();
        logLine(LogLevel::Info, "server.stopped", "signal=" + std::to_string(shutdownSignal),
                "clients=" + std::to_string(clients.size()));
        close(listener);
        return 0;
    }

private:
    ReadinessDispatcher &dispatcher;
    int listener = -1;
    std::unordered_map<int, ClientConnection> clients;
    std::vector<ChatRoom> rooms;

    bool queueOutput(ClientConnection *client, const char *data, size_t size) {
        bool queued = queueOutputCommon(client, data, size, [&](bool enabled) {
            return dispatcher.setWriteInterest(client->fd, enabled);
        });
        if (!queued) {
            LogLevel level = errno == ENOBUFS ? LogLevel::Warning : LogLevel::Error;
            logLine(level, "socket.output_rejected", "fd=" + std::to_string(client->fd),
                    "bytes=" + std::to_string(size),
                    "pending_bytes=" + std::to_string(client->outputQueueSize),
                    "errno=" + std::to_string(errno));
        }
        return queued;
    }

    bool flushOutput(ClientConnection *client) {
        return flushOutputCommon(client, [&](bool enabled) {
            return dispatcher.setWriteInterest(client->fd, enabled);
        });
    }

    void removeFromRooms(int fd) {
        for (ChatRoom &room : rooms) {
            room.subscribedClients.erase(
                std::remove(room.subscribedClients.begin(), room.subscribedClients.end(), fd),
                room.subscribedClients.end());
        }
    }

    void deleteEmptyRoom(int roomId) {
        auto room = std::find_if(rooms.begin(), rooms.end(), [&](const ChatRoom &candidate) {
            return candidate.roomIdx == roomId;
        });
        if (room != rooms.end() && room->roomName != "main-room" &&
            room->subscribedClients.empty()) {
            rooms.erase(room);
        }
    }

    void disconnect(int fd, const std::string &reason) {
        auto client = clients.find(fd);
        if (client == clients.end()) {
            return;
        }
        const int oldRoom = client->second.currRoom;
        const size_t pending = client->second.outputQueueSize;
        const size_t peak = client->second.peakPendingOutputBytes;
        const std::string username = client->second.username;
        removeFromRooms(fd);
        if (!dispatcher.remove(fd) && errno != ENOENT) {
            logError("dispatcher.remove_failed", errno, fd);
        }
        if (close(fd) == -1) {
            logError("socket.close_failed", errno, fd);
        }
        clients.erase(client);
        deleteEmptyRoom(oldRoom);
        logLine(LogLevel::Info, "client.disconnected", "fd=" + std::to_string(fd),
                "username=" + logValue(username), "reason=" + logValue(reason),
                "pending_bytes=" + std::to_string(pending),
                "peak_pending_bytes=" + std::to_string(peak),
                "clients=" + std::to_string(clients.size()));
    }

    std::string roomName(int roomId) const {
        auto room = std::find_if(rooms.begin(), rooms.end(), [&](const ChatRoom &candidate) {
            return candidate.roomIdx == roomId;
        });
        return room == rooms.end() ? std::string{} : room->roomName;
    }

    void joinRoom(ClientConnection *client, const std::string &name) {
        auto room = std::find_if(rooms.begin(), rooms.end(), [&](const ChatRoom &candidate) {
            return candidate.roomName == name;
        });
        if (room == rooms.end()) {
            int nextId = 0;
            for (const ChatRoom &candidate : rooms) {
                nextId = std::max(nextId, candidate.roomIdx + 1);
            }
            rooms.push_back({nextId, name, {}});
            room = std::prev(rooms.end());
        }
        removeFromRooms(client->fd);
        room->subscribedClients.push_back(client->fd);
        client->currRoom = room->roomIdx;
    }

    void notifyRoom(int roomId, int excludedFd, const std::string &message) {
        auto room = std::find_if(rooms.begin(), rooms.end(), [&](const ChatRoom &candidate) {
            return candidate.roomIdx == roomId;
        });
        if (room == rooms.end()) {
            return;
        }
        const std::vector<int> recipients = room->subscribedClients;
        for (int fd : recipients) {
            if (fd == excludedFd) {
                continue;
            }
            auto client = clients.find(fd);
            if (client == clients.end() || client->second.clientState != CLIENT_ACTIVE ||
                client->second.currRoom != roomId) {
                continue;
            }
            if (!queueOutput(&client->second, message.data(), message.size())) {
                disconnect(fd, "slow client: room notification output queue overflow");
            }
        }
    }

    void acceptConnections() {
        while (true) {
            sockaddr_storage address{};
            socklen_t addressSize = sizeof address;
            int fd = accept(listener, reinterpret_cast<sockaddr *>(&address), &addressSize);
            if (fd == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return;
                }
                if (errno == EINTR) {
                    continue;
                }
                logError("socket.accept_failed", errno, listener);
                return;
            }
            if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1 || !dispatcher.add(fd)) {
                logError("socket.add_failed", errno, fd);
                close(fd);
                continue;
            }
            ClientConnection connection{};
            connection.fd = fd;
            connection.clientState = CLIENT_AWAITING_USERNAME;
            auto client = clients.emplace(fd, std::move(connection)).first;
            constexpr char prompt[] = "enter your username: ";
            if (!queueOutput(&client->second, prompt, sizeof prompt - 1)) {
                disconnect(fd, "username prompt output queue overflow");
                continue;
            }
            char remote[INET6_ADDRSTRLEN];
            const char *text = addressText(&address, remote, sizeof remote);
            logLine(LogLevel::Info, "client.accepted", "fd=" + std::to_string(fd),
                    "remote=" + logValue(text == nullptr ? "unknown" : text),
                    "clients=" + std::to_string(clients.size()));
        }
    }

    static std::vector<std::string> tokens(const std::string &input) {
        std::istringstream stream(input);
        std::vector<std::string> result;
        std::string token;
        while (getline(stream, token, ' ')) {
            result.push_back(token);
        }
        return result;
    }

    bool listRooms(ClientConnection *client) {
        std::string output = "+------+----------------+\n| ID   | Room           |\n+------+----------------+\n";
        for (const ChatRoom &room : rooms) {
            output += "| " + std::to_string(room.roomIdx) + "\t| " + room.roomName + "\n";
        }
        output += "+------+----------------+\n";
        return queueOutput(client, output.data(), output.size());
    }

    bool processFrame(ClientConnection *sender, const std::string &frame) {
        if (frame.empty()) {
            return true;
        }
        static const std::unordered_set<std::string> commands = {
            "LIST", "JOIN", "LEAVE", "QUIT", "WHERE"
        };
        const std::vector<std::string> words = tokens(frame);
        const bool command = !words.empty() && commands.count(words[0]) != 0;
        logLine(LogLevel::Debug, "frame.received", "fd=" + std::to_string(sender->fd),
                "bytes=" + std::to_string(frame.size()),
                "type=" + logValue(command ? words[0] : "message"));

        if (!command) {
            return broadcast(sender, words);
        }
        if (words[0] == "LIST") {
            if (!listRooms(sender)) {
                disconnect(sender->fd, "slow client: LIST response output queue overflow");
                return false;
            }
            return true;
        }
        if (words[0] == "JOIN" && words.size() == 2) {
            const int oldRoom = sender->currRoom;
            const std::string oldName = roomName(oldRoom);
            if (oldName != words[1]) {
                notifyRoom(oldRoom, sender->fd,
                           "> server: " + sender->username + " left room " + oldName + "\n");
            }
            joinRoom(sender, words[1]);
            if (oldName != words[1]) {
                notifyRoom(sender->currRoom, -1,
                           "> server: " + sender->username + " joined room " + words[1] + "\n");
            }
            deleteEmptyRoom(oldRoom);
            return true;
        }
        if (words[0] == "LEAVE" && sender->currRoom != 0) {
            const int oldRoom = sender->currRoom;
            const std::string oldName = roomName(oldRoom);
            notifyRoom(oldRoom, sender->fd,
                       "> server: " + sender->username + " left room " + oldName + "\n");
            joinRoom(sender, "main-room");
            notifyRoom(sender->currRoom, -1,
                       "> server: " + sender->username + " joined room main-room\n");
            deleteEmptyRoom(oldRoom);
            return true;
        }
        if (words[0] == "WHERE" && words.size() == 1) {
            std::string name = roomName(sender->currRoom);
            std::string output = name.empty() ? "> server: room not found\n"
                                              : "> server: you are in room " + name + "\n";
            if (!queueOutput(sender, output.data(), output.size())) {
                disconnect(sender->fd, "slow client: WHERE response output queue overflow");
                return false;
            }
            return true;
        }
        if (words[0] == "QUIT") {
            const int fd = sender->fd;
            notifyRoom(sender->currRoom, fd,
                       "> server: " + sender->username + " has disconnected\n");
            disconnect(fd, "client requested QUIT");
            return false;
        }
        return true;
    }

    bool broadcast(ClientConnection *sender, const std::vector<std::string> &words) {
        std::string output = "> " + sender->username + ": ";
        for (const std::string &word : words) {
            output += word + ' ';
        }
        output += '\n';
        auto room = std::find_if(rooms.begin(), rooms.end(), [&](const ChatRoom &candidate) {
            return candidate.roomIdx == sender->currRoom;
        });
        if (room == rooms.end()) {
            return false;
        }
        size_t recipients = 0;
        const std::vector<int> destinations = room->subscribedClients;
        for (int fd : destinations) {
            if (fd == sender->fd) {
                continue;
            }
            auto client = clients.find(fd);
            if (client == clients.end() || client->second.clientState != CLIENT_ACTIVE ||
                client->second.currRoom != sender->currRoom) {
                continue;
            }
            if (!queueOutput(&client->second, output.data(), output.size())) {
                disconnect(fd, "slow client: chat message output queue overflow");
                return false;
            }
            recipients++;
        }
        logLine(LogLevel::Debug, "chat.broadcast_queued", "fd=" + std::to_string(sender->fd),
                "room_id=" + std::to_string(sender->currRoom),
                "bytes=" + std::to_string(output.size()),
                "recipients=" + std::to_string(recipients));
        return true;
    }

    void handleRead(int fd) {
        auto found = clients.find(fd);
        if (found == clients.end()) {
            return;
        }
        ClientConnection *client = &found->second;
        char buffer[MAX_DATA_SIZE];
        size_t bytesRead = 0;
        size_t framesProcessed = 0;
        while (bytesRead < MAX_BYTES_READ_PER_POLL &&
               framesProcessed < MAX_PROCESSED_FRAMES_PER_POLL) {
            ssize_t count = recv(fd, buffer, sizeof buffer, 0);
            const int receiveError = errno;
            recordReceiveResult(count, receiveError);
            if (count > 0) {
                bytesRead += static_cast<size_t>(count);
                client->inputBuffer.append(buffer, static_cast<size_t>(count));
                logLine(LogLevel::Debug, "socket.received", "fd=" + std::to_string(fd),
                        "bytes=" + std::to_string(count),
                        "buffered_bytes=" + std::to_string(client->inputBuffer.size()));
                if (client->inputBuffer.size() > MAX_INPUT_BUFFER_BYTES) {
                    disconnect(fd, "input buffer limit exceeded");
                    return;
                }
                while (framesProcessed < MAX_PROCESSED_FRAMES_PER_POLL) {
                    size_t newline = client->inputBuffer.find('\n');
                    if (newline == std::string::npos) {
                        if (client->inputBuffer.size() > MAX_FRAME_BYTES) {
                            disconnect(fd, "input frame limit exceeded");
                            return;
                        }
                        break;
                    }
                    if (newline > MAX_FRAME_BYTES) {
                        disconnect(fd, "input frame limit exceeded");
                        return;
                    }
                    std::string frame = client->inputBuffer.substr(0, newline);
                    client->inputBuffer.erase(0, newline + 1);
                    if (!frame.empty() && frame.back() == '\r') {
                        frame.pop_back();
                    }
                    if (client->clientState == CLIENT_AWAITING_USERNAME) {
                        client->username = frame;
                        client->clientState = CLIENT_ACTIVE;
                        joinRoom(client, "main-room");
                        notifyRoom(client->currRoom, -1,
                                   "> server: " + client->username + " joined room main-room\n");
                        if (clients.find(fd) == clients.end()) {
                            return;
                        }
                        logLine(LogLevel::Info, "client.authenticated",
                                "fd=" + std::to_string(fd),
                                "username=" + logValue(client->username),
                                "room_id=" + std::to_string(client->currRoom));
                    } else {
                        framesProcessed++;
                        if (!processFrame(client, frame)) {
                            return;
                        }
                        if (clients.find(fd) == clients.end()) {
                            return;
                        }
                    }
                }
                continue;
            }
            if (count == 0) {
                notifyRoom(client->currRoom, fd,
                           "> server: " + client->username + " disconnected\n");
                disconnect(fd, "peer closed connection");
                return;
            }
            if (receiveError == EAGAIN || receiveError == EWOULDBLOCK ||
                receiveError == EINTR) {
                return;
            }
            logError("socket.receive_failed", receiveError, fd);
            notifyRoom(client->currRoom, fd,
                       "> server: " + client->username + " disconnected\n");
            disconnect(fd, "recv failed");
            return;
        }
    }

    void handleEvent(const ReadyEvent &event) {
        logLine(LogLevel::Debug, "dispatcher.event", "fd=" + std::to_string(event.fd),
                "flags=" + std::to_string(event.flags));
        if (event.fd == listener) {
            if (event.flags & READY_READ) {
                acceptConnections();
            }
            return;
        }
        if (event.flags & (READY_READ | READY_ERROR)) {
            handleRead(event.fd);
        }
        auto client = clients.find(event.fd);
        if ((event.flags & READY_WRITE) && client != clients.end() &&
            !flushOutput(&client->second)) {
            const std::string username = client->second.username;
            const int room = client->second.currRoom;
            notifyRoom(room, event.fd, "> server: " + username + " disconnected\n");
            disconnect(event.fd, "send failed");
        }
    }
};
}

int runChatServer(ReadinessDispatcher &dispatcher) {
    struct sigaction action{};
    action.sa_handler = handleShutdown;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);
    installMetricsSignalHandlers();
    ChatServer server(dispatcher);
    return server.run();
}
