CXX ?= g++
COMMON_FLAGS := -std=c++17 -Wall -Wextra -pedantic
RELEASE_FLAGS := $(COMMON_FLAGS) -O2 -DNDEBUG
DEBUG_FLAGS := $(COMMON_FLAGS) -O0 -g
ASAN_FLAGS := $(COMMON_FLAGS) -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer

.PHONY: all release debug asan poll epoll compare compare-smoke test clean

all: release

release: poll epoll

poll:
	$(MAKE) -C 'poll(2)' server CXX='$(CXX)' RELEASE_FLAGS='$(RELEASE_FLAGS)'

epoll:
	$(MAKE) -C epoll server CXX='$(CXX)' RELEASE_FLAGS='$(RELEASE_FLAGS)'

debug:
	$(MAKE) -C 'poll(2)' server_debug CXX='$(CXX)' DEBUG_FLAGS='$(DEBUG_FLAGS)'
	$(MAKE) -C epoll server_debug CXX='$(CXX)' DEBUG_FLAGS='$(DEBUG_FLAGS)'

asan:
	$(MAKE) -C 'poll(2)' server_asan CXX='$(CXX)' ASAN_FLAGS='$(ASAN_FLAGS)'
	$(MAKE) -C epoll server_asan CXX='$(CXX)' ASAN_FLAGS='$(ASAN_FLAGS)'

compare: release
	python3 bench/compare.py

compare-smoke: release
	python3 bench/compare.py --name smoke --tiers 10,100 --runs 2 --duration 1 --warmup 0.2 --drain 0.5 --rate 100

test: release
	python3 -m unittest discover -s tests -v

clean:
	$(MAKE) -C 'poll(2)' clean
	$(MAKE) -C epoll clean
