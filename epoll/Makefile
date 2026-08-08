CXX ?= g++
COMMON_FLAGS ?= -std=c++17 -Wall -Wextra -pedantic
RELEASE_FLAGS ?= $(COMMON_FLAGS) -O2 -DNDEBUG
DEBUG_FLAGS ?= $(COMMON_FLAGS) -O0 -g
ASAN_FLAGS ?= $(COMMON_FLAGS) -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer

.PHONY: all release debug asan benchmark benchmark-smoke clean

all: release client

release: server

debug: server_debug

asan: server_asan

benchmark: server
	python3 ../bench/compare.py --no-build --implementations epoll --name epoll --experiments sparse

benchmark-smoke: server
	python3 ../bench/compare.py --no-build --implementations epoll --name epoll-smoke --experiments sparse --sparse-tiers 10 --sparse-active 1,5 --runs 1 --warmup 0.1 --duration 0.5 --drain 0.5

SERVER_SOURCES := epoll_dispatcher.cpp ../common/chat_server.cpp ../common/server_common.cpp
SERVER_HEADERS := ../common/chat_server.hpp ../common/server_common.hpp

server_asan: $(SERVER_SOURCES) $(SERVER_HEADERS) Makefile
	$(CXX) $(ASAN_FLAGS) $(SERVER_SOURCES) -o server_asan

server_debug: $(SERVER_SOURCES) $(SERVER_HEADERS) Makefile
	$(CXX) $(DEBUG_FLAGS) $(SERVER_SOURCES) -o server_debug

server: $(SERVER_SOURCES) $(SERVER_HEADERS) Makefile
	$(CXX) $(RELEASE_FLAGS) $(SERVER_SOURCES) -o server

client: client_epoll.cpp Makefile
	$(CXX) $(RELEASE_FLAGS) client_epoll.cpp -o client

clean:
	rm -f server server_debug server_asan client
