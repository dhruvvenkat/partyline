CXX ?= g++
COMMON_FLAGS := -std=c++17 -Wall -Wextra -pedantic
RELEASE_FLAGS := $(COMMON_FLAGS) -O2 -DNDEBUG
DEBUG_FLAGS := $(COMMON_FLAGS) -O0 -g
ASAN_FLAGS := $(COMMON_FLAGS) -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer

.PHONY: all release debug asan poll epoll compare compare-smoke compare-sparse compare-dense compare-broadcast queue-test test clean

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
	python3 bench/compare.py --no-build

compare-smoke: release
	python3 bench/compare.py --no-build --name smoke --runs 1 --duration 0.5 --warmup 0.1 --drain 0.5 --sparse-tiers 10 --sparse-active 1,5 --sparse-rate 1000 --dense-tiers 10 --dense-rates 1000,5000 --broadcast-tiers 10 --broadcast-rates 100,500 --workers 2

compare-sparse: release
	python3 bench/compare.py --no-build --experiments sparse

compare-dense: release
	python3 bench/compare.py --no-build --experiments dense

compare-broadcast: release
	python3 bench/compare.py --no-build --experiments broadcast

queue-test:
	$(CXX) $(DEBUG_FLAGS) -I. tests/queue_fast_path_test.cpp common/server_common.cpp -o /tmp/event_chat_queue_fast_path_test
	/tmp/event_chat_queue_fast_path_test

test: release queue-test
	python3 -m unittest discover -s tests -v

clean:
	$(MAKE) -C 'poll(2)' clean
	$(MAKE) -C epoll clean
