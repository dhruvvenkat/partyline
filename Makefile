all: server client

.PHONY: asan benchmark

benchmark: server
	python3 bench/benchmark.py

asan: server_asan

server_asan: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic -g -O1 -fsanitize=address -fno-omit-frame-pointer server.cpp network.cpp -o server_asan

server: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic server.cpp network.cpp -o server

client: client.cpp
	g++ -std=c++17 -Wall -Wextra -pedantic client.cpp -o client
