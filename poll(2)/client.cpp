#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <netdb.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>

constexpr char DEFAULT_HOST[] = "127.0.0.1";
constexpr char DEFAULT_PORT[] = "1234";
constexpr char USERNAME_PROMPT[] = "enter your username: ";
constexpr int CLIENT_BUFFER_SIZE = 4096;
constexpr size_t MAX_VISIBLE_MESSAGES = 24;

static termios savedTerminal{};
static bool terminalIsRaw = false;

void restoreTerminal() {
    if (terminalIsRaw) {
        tcsetattr(STDIN_FILENO, TCSANOW, &savedTerminal);
        terminalIsRaw = false;
    }
}

void handleInterrupt(int) {
    restoreTerminal();
    _exit(130);
}

bool enableRawTerminal() {
    if (!isatty(STDIN_FILENO) || tcgetattr(STDIN_FILENO, &savedTerminal) == -1) {
        return false;
    }

    termios raw = savedTerminal;
    raw.c_lflag &= ~(ICANON | ECHO);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;

    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) == -1) {
        return false;
    }

    terminalIsRaw = true;
    std::atexit(restoreTerminal);
    std::signal(SIGINT, handleInterrupt);
    return true;
}

void addMessage(std::deque<std::string> &messages, std::string message) {
    if (!message.empty()) {
        messages.push_back(std::move(message));
    }
    while (messages.size() > MAX_VISIBLE_MESSAGES) {
        messages.pop_front();
    }
}

void render(const std::deque<std::string> &messages, const std::string &room, const std::string &input, bool usernameMode, bool quitting) {
    std::cout << "\033[2J\033[H"
              << "================ EVENT CHAT ================\n"
              << " room: " << room << "\n"
              << "----------------------------------------------\n";

    for (const auto &message : messages) {
        std::cout << message << '\n';
    }

    std::cout << "----------------------------------------------\n";
    if (quitting) {
        std::cout << " disconnecting...";
    } else if (usernameMode) {
        std::cout << " username: " << input;
    } else {
        std::cout << "> " << input;
    }
    std::cout << std::flush;
}

bool sendAll(int fd, const std::string &message) {
    size_t sent = 0;
    while (sent < message.size()) {
        ssize_t numBytes = send(fd, message.data() + sent, message.size() - sent, MSG_NOSIGNAL);
        if (numBytes > 0) {
            sent += numBytes;
        } else if (numBytes == -1 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

int connectToServer(const char *host, const char *port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *results = nullptr;
    int status = getaddrinfo(host, port, &hints, &results);
    if (status != 0) {
        std::cerr << "client: " << gai_strerror(status) << '\n';
        return -1;
    }

    int fd = -1;
    for (addrinfo *result = results; result != nullptr; result = result->ai_next) {
        fd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
        if (fd == -1) {
            continue;
        }

        if (connect(fd, result->ai_addr, result->ai_addrlen) == 0) {
            break;
        }

        close(fd);
        fd = -1;
    }

    freeaddrinfo(results);
    return fd;
}

bool receiveServerData(int fd, std::string &received, std::deque<std::string> &messages) {
    char buffer[CLIENT_BUFFER_SIZE];
    ssize_t numBytes = recv(fd, buffer, sizeof buffer, 0);

    if (numBytes > 0) {
        received.append(buffer, numBytes);

        if (received.compare(0, sizeof USERNAME_PROMPT - 1, USERNAME_PROMPT) == 0) {
            received.erase(0, sizeof USERNAME_PROMPT - 1);
        }

        size_t newline;
        while ((newline = received.find('\n')) != std::string::npos) {
            addMessage(messages, received.substr(0, newline));
            received.erase(0, newline + 1);
        }
        return true;
    }

    if (numBytes == -1 && errno == EINTR) {
        return true;
    }
    return false;
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : DEFAULT_HOST;
    const char *port = argc > 2 ? argv[2] : DEFAULT_PORT;
    int serverfd = connectToServer(host, port);

    if (serverfd == -1) {
        std::cerr << "client: unable to connect to " << host << ':' << port << '\n';
        return 1;
    }

    if (!enableRawTerminal()) {
        std::cerr << "client: an interactive terminal is required\n";
        close(serverfd);
        return 1;
    }

    std::deque<std::string> messages;
    std::string received;
    std::string input;
    std::string room = "(awaiting username)";
    bool usernameMode = true;
    bool quitting = false;
    bool running = true;
    addMessage(messages, "Connected to " + std::string(host) + ":" + port);

    while (running) {
        render(messages, room, input, usernameMode, quitting);

        pollfd pfds[2] = {
            {serverfd, POLLIN, 0},
            {STDIN_FILENO, static_cast<short>(quitting ? 0 : POLLIN), 0},
        };

        int pollCount = poll(pfds, 2, -1);
        if (pollCount == -1 && errno == EINTR) {
            continue;
        }
        if (pollCount == -1) {
            addMessage(messages, "poll failed");
            break;
        }

        if (pfds[0].revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) {
            if (!receiveServerData(serverfd, received, messages)) {
                addMessage(messages, "server disconnected");
                break;
            }
        }

        if (pfds[1].revents & POLLIN) {
            char buffer[128];
            ssize_t numBytes = read(STDIN_FILENO, buffer, sizeof buffer);
            if (numBytes <= 0) {
                break;
            }

            for (ssize_t i = 0; i < numBytes; i++) {
                unsigned char ch = buffer[i];

                // End-of-text ASCII value
                if (ch == 3) {
                    running = false;
                    break;
                }

                // End-of-transmission ASCII value
                if (ch == 4 && input.empty()) {
                    running = false;
                    break;
                }

                if (ch == '\r' || ch == '\n') {
                    std::string line = input;
                    input.clear();

                    if (usernameMode) {
                        usernameMode = false;
                        room = "main-room";
                    } else if (line == "/help") {
                        addMessage(messages, "Commands: /join <room>, /leave, /list, /where, /quit");
                        continue;
                    } else if (line == "/quit" || line == "QUIT") {
                        line = "QUIT";
                        quitting = true;
                    } else if (line == "/leave") {
                        line = "LEAVE";
                        room = "main-room";
                    } else if (line == "/list") {
                        line = "LIST";
                    } else if (line == "/where") {
                        line = "WHERE";
                    } else if (line.rfind("/join ", 0) == 0) {
                        room = line.substr(6);
                        line = "JOIN " + room;
                    } else if (!line.empty() && line[0] == '/') {
                        addMessage(messages, "Unknown command; use /help");
                        continue;
                    } else if (!usernameMode) {
                        addMessage(messages, "> you: " + line);
                    }

                    line.push_back('\n');
                    if (!sendAll(serverfd, line)) {
                        addMessage(messages, "send failed");
                        running = false;
                    }
                    continue;
                }

                if (ch == 127 || ch == '\b') {
                    if (!input.empty()) {
                        input.pop_back();
                    }
                } else if (std::isprint(ch)) {
                    input.push_back(static_cast<char>(ch));
                }
            }
        }
    }

    restoreTerminal();
    std::cout << "\n";
    close(serverfd);
    return 0;
}
