all: server client

.PHONY: asan benchmark benchmark-smoke

benchmark: server
	python3 bench/benchmark.py --output bench/results/poll.csv

benchmark-smoke: server
	python3 bench/benchmark.py --tiers 2,10 --runs 1 --warmup 0.5 --duration 1

asan: server_asan

server_asan: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic -g -O1 -fsanitize=address -fno-omit-frame-pointer server.cpp network.cpp -o server_asan

server: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic server.cpp network.cpp -o server

client: client.cpp
	g++ -std=c++17 -Wall -Wextra -pedantic client.cpp -o client
