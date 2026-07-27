server: server.cpp network.cpp server.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic server.cpp network.cpp -o server
