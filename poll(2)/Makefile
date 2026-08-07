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
	python3 ../bench/compare.py --implementations poll --name poll

benchmark-smoke: server
	python3 ../bench/compare.py --implementations poll --name poll-smoke --tiers 10 --runs 1 --warmup 0.2 --duration 1 --drain 0.5

server_asan: server.cpp network.cpp server.hpp ../common/server_common.cpp ../common/server_common.hpp Makefile
	$(CXX) $(ASAN_FLAGS) server.cpp network.cpp ../common/server_common.cpp -o server_asan

server_debug: server.cpp network.cpp server.hpp ../common/server_common.cpp ../common/server_common.hpp Makefile
	$(CXX) $(DEBUG_FLAGS) server.cpp network.cpp ../common/server_common.cpp -o server_debug

server: server.cpp network.cpp server.hpp ../common/server_common.cpp ../common/server_common.hpp Makefile
	$(CXX) $(RELEASE_FLAGS) server.cpp network.cpp ../common/server_common.cpp -o server

client: client.cpp Makefile
	$(CXX) $(RELEASE_FLAGS) client.cpp -o client

clean:
	rm -f server server_debug server_asan client
