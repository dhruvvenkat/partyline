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
	python3 ../bench/compare.py --implementations epoll --name epoll

benchmark-smoke: server
	python3 ../bench/compare.py --implementations epoll --name epoll-smoke --tiers 10 --runs 1 --warmup 0.2 --duration 1 --drain 0.5

server_asan: server_epoll.cpp network_epoll.cpp server_epoll.hpp Makefile
	$(CXX) $(ASAN_FLAGS) server_epoll.cpp network_epoll.cpp -o server_asan

server_debug: server_epoll.cpp network_epoll.cpp server_epoll.hpp Makefile
	$(CXX) $(DEBUG_FLAGS) server_epoll.cpp network_epoll.cpp -o server_debug

server: server_epoll.cpp network_epoll.cpp server_epoll.hpp Makefile
	$(CXX) $(RELEASE_FLAGS) server_epoll.cpp network_epoll.cpp -o server

client: client_epoll.cpp Makefile
	$(CXX) $(RELEASE_FLAGS) client_epoll.cpp -o client

clean:
	rm -f server server_debug server_asan client
