all: server client

.PHONY: asan benchmark benchmark-smoke

benchmark: server
	python3 bench/benchmark.py --label poll-unlimited --tiers 10,50,100,500 --workloads direct --rate 100 --output bench/results/poll-unlimited-direct-r100.csv
	python3 bench/benchmark.py --label poll-unlimited --tiers 10 --workloads broadcast --rate 2000 --output bench/results/poll-unlimited-broadcast-10-r2000.csv
	python3 bench/benchmark.py --label poll-unlimited --tiers 50 --workloads broadcast --rate 200 --output bench/results/poll-unlimited-broadcast-50-r200.csv
	python3 bench/benchmark.py --label poll-unlimited --tiers 100 --workloads broadcast --rate 60 --output bench/results/poll-unlimited-broadcast-100-r60.csv
	python3 bench/benchmark.py --label poll-unlimited --tiers 500 --workloads broadcast --rate 2 --output bench/results/poll-unlimited-broadcast-500-r2.csv

benchmark-smoke: server
	python3 bench/benchmark.py --tiers 2,10 --runs 1 --warmup 0.5 --duration 1

asan: server_asan

server_asan: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic -g -O1 -fsanitize=address -fno-omit-frame-pointer server.cpp network.cpp -o server_asan

server: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic server.cpp network.cpp -o server

client: client.cpp
	g++ -std=c++17 -Wall -Wextra -pedantic client.cpp -o client

clean:
	rm server client server_asan
