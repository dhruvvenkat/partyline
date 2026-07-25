#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <cctype>
#include <csignal>

#define QUEUE_LENGTH 10
#define MAX_DATA_SIZE 100

enum Command {
	SET,
	GET,
	DELETE,
	EXIT
};

void signalHandler(int sig) {
    if (sig == 2) {
        std::cout << "\nServer interupted, closing..." << std::endl;
        exit(sig);
    }

    std::cout << "Interrupt handle " << sig << std::endl;
    exit(sig);
}

void tokenizeBySpaces(const std::string &input, std::vector<std::string> &tokens) {
	std::istringstream iss(input);
	std::string buf;

	while (getline(iss, buf, ' ')) {
		tokens.push_back(buf);
	}

	return;
}

struct ClientSession {
    int socketfd;
    ssize_t numBytes;
    char buf[MAX_DATA_SIZE];
    std::string pending;
    std::vector<std::string> tokens;

    std::string cmd;
    std::string key;
    std::string value;
};

class Server {
    int sockfd;
    struct addrinfo hints, *servinfo, *ptr;

    socklen_t incomingSize;
    char s[INET6_ADDRSTRLEN];
    int rv;

    int yes = 1;

    char errCmd[sizeof("ERROR: send messages in the form {COMMAND KEY VALUE}\ne.g. SET name dhruv\n")] = "ERROR: send messages in the form {COMMAND KEY VALUE}\ne.g. SET name dhruv\n";
    char keyNotFoundErr[sizeof("ERROR: requested key not found in store\n")] = "ERROR: requested key not found in store\n";

    std::unordered_map<std::string, std::string> pairs;

    int setupSocket(char *port) {
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_INET; // Limit to IPv4 addresses only
        hints.ai_socktype = SOCK_STREAM; // Define TCP socket
        hints.ai_flags = AI_PASSIVE; // Serve from the local machine's IP address

        rv = getaddrinfo(NULL, port, &hints, &servinfo);
        if (rv != 0) {
            std::cerr << "getaddrinfo: " << gai_strerror(rv);
            return 1;
        }

        for (ptr = servinfo; ptr != NULL; ptr = ptr->ai_next) {
            // Creating the socket connection using the derived addrinfo struct and ensuring that it is valid
            sockfd = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol);
            if (sockfd == -1) {
                perror("server: socket");
                continue;
            }

            // Configure socket to allow other active non-listening sockets to bind() to this port
            if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
                perror("server: setsockopt");
                return 1;
            }

            if (bind(sockfd, ptr->ai_addr, ptr->ai_addrlen) == -1) {
                close(sockfd);
                perror("server: bind()");
                continue;
            }

            break;
        }

        freeaddrinfo(servinfo); // Socket has been configured so we no longer need metadata about the client

        if (ptr == NULL) {
            std::cerr << "server: no compatible addresses to bind to" << std::endl;
            return 1;
        }

        if (listen(sockfd, QUEUE_LENGTH) == -1) {
            perror("listen");
            return 1;
        }

        std::cout << "server started on port " << port << "..." << std::endl;
        std::cout << "server: waiting for connection..." << std::endl;
        return 0;
    }

    int acceptConnection() {
        struct sockaddr_storage incomingAddr;
        incomingSize = sizeof incomingAddr;
        int incomingfd = accept(sockfd, (struct sockaddr *)&incomingAddr, &incomingSize);
        if (incomingfd == -1) {
            perror("accept");
            return -1;
        }

        // Converting the incoming IP address from pure binary to a human-readable format
        inet_ntop(incomingAddr.ss_family, (struct sockaddr *)&incomingAddr, s, sizeof s);
        std::cout << "server: receieved connection from " << s << std::endl;
        return incomingfd;
    }

    bool processGet(ClientSession& client) {
        try {
            std::string associatedVal;
            associatedVal = pairs.at(client.key);

            std::string getResponse = "Key: " + client.key + "\tValue: " + associatedVal + "\n";
            if (send(client.socketfd, getResponse.data(), getResponse.size(), 0) == -1) {
                perror("server: send");
            }
        } catch (std::out_of_range) {
            if (send(client.socketfd, keyNotFoundErr, sizeof keyNotFoundErr, 0) == -1) {
                perror("server: send key not found error");
                return true;
            }
        }

        return false;
    }

    bool processDelete(ClientSession& client) {
        std::size_t erased = pairs.erase(client.key);

        if (erased == 0) {
            if (send(client.socketfd, keyNotFoundErr, sizeof keyNotFoundErr, 0) == -1) {
                perror("server: send key not found error");
                return true;
            }
        } else if (erased == 1) {
            std::string deleteMsg = "successfully deleted entry with key " + client.key + "\n";

            if (send(client.socketfd, deleteMsg.data(), deleteMsg.size(), 0) == -1) {
                perror("server: send key not found error");
                return true;
            }
        }

        return false;
    }

    bool processNotCommand(ClientSession& client) {
        // TODO: send this out instead of just printing it on server side
        std::string notCmdErr = "ERROR: NOT A COMMAND\n";
        if (send(client.socketfd, notCmdErr.data(), notCmdErr.size(), 0) == -1) {
            perror("server: send key not found error");
            return true;
        }

        return false;
    }

    bool processSet(ClientSession& client) {
        // check if the key already exists in the entries and just return that if it is
        std::string associatedVal;
        bool keyExists = true;

        try {
            associatedVal = pairs.at(client.key);
        } catch (std::out_of_range) {
            pairs.insert({client.key, client.value});
            keyExists = false;
        }

        if (keyExists) {
            std::string getResponse = "Your entry already exists with key: " + client.key + " and value: " + associatedVal + "\n";
            if (send(client.socketfd, getResponse.c_str(), getResponse.size(), 0) == -1) {
                perror("server: send setResponse (key found)");
            }
        } else {
            std::string insertMsg = "Entry with key: " + client.key + " and value: " + client.value + " has been inserted successfully\n";

            if (send(client.socketfd, insertMsg.data(), insertMsg.size(), 0) == -1) {
                perror("server: send setResponse (key found)");
            }
        }

        return false;
    }

    bool processTwoTokenCommand(ClientSession& client) {
        if (client.cmd == "GET") {
            return processGet(client);
        } else if (client.cmd == "DELETE") {
            return processDelete(client);
        } else {
            return processNotCommand(client);
        }
    }

    bool processValueCommand(ClientSession& client) {
        for (int i = 2; i < client.tokens.size(); i++) {
            client.value.append(client.tokens[i]);
            client.value.append(" ");
        }

        std::cout << "CMD: " << client.cmd << "  KEY: " << client.key << "  VAL: " << client.value << std::endl;

        if (client.cmd == "SET") {
            return processSet(client);
        } else {
            return processNotCommand(client);
        }
    }

    int processCommand(const std::string &command, ClientSession& client) {
        tokenizeBySpaces(command, client.tokens);
        std::cout << "server: receieved command " << command << '\n';

        if (client.tokens.size() < 2) {
            if ((send(client.socketfd, errCmd, strlen(errCmd), 0)) == -1) {
                perror("send");
                return 2;
            }

            client.tokens.clear();
            return 1;
        }

        client.cmd = client.tokens[0];
        client.key = client.tokens[1];

        if (client.tokens.size() == 2) {
            return processTwoTokenCommand(client) ? 1 : 0;
        }

        return processValueCommand(client) ? 1 : 0;
    }

    bool processPendingCommands(ClientSession& client) {
        std::size_t newlinePos;
        bool shouldBreak = false;

        // Commands are newline-delimited to avoid issues with TCP latency breaking up commands
        while ((newlinePos = client.pending.find('\n')) != std::string::npos) {
            std::string command = client.pending.substr(0, newlinePos);
            client.pending.erase(0, newlinePos+1);

            if (command == "exit") {
                if (send(client.socketfd, "closing connection...\n", sizeof("closing connection...\n") - 1, 0) == -1) {
                    perror("send");
                }

                shouldBreak = true;
                break;
            }

            int commandResult = processCommand(command, client);
            if (commandResult == 2) {
                continue;
            }

            if (commandResult == 1) {
                break;
            }

            client.cmd.clear();
            client.key.clear();
            client.value.clear();
            client.tokens.clear();
        }

        return shouldBreak;
    }

    void handleConnection(int clientFd) {
        ClientSession client{};
        client.socketfd = clientFd;

        while (true) {
            client.numBytes = recv(
                client.socketfd,
                client.buf,
                sizeof client.buf,
                0
            );

            if (client.numBytes == 0) {
                std::cout << "client disconnected\n";
                break;
            }

            if (client.numBytes == -1) {
                perror("recv");
                break;
            }

            client.pending.append(client.buf, client.numBytes);

            if (processPendingCommands(client)) {
                break;
            }
        }

        close(client.socketfd);
    }

public:
    int run(char *port) {
        if (setupSocket(port) != 0) {
            return 1;
        }

        while (1) {
            int incomingfd = acceptConnection();

            if (incomingfd == -1) {
                continue;
            }

            handleConnection(incomingfd);
        }

        close(sockfd);
        return 0;
    }
};

int main(int argc, char *argv[]) {
    signal(SIGINT, signalHandler);

    if (argc < 2) {
        std::cerr << "usage: ./server port" << std::endl;
        return -1;
    }

    char *portStr = argv[1];

    Server server;
    return server.run(portStr);
}
