/****************************************************
 *
 * Testing for Fault Tolerance API
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <chrono>
#include <stdio.h>
#include <iostream>
#include <getopt.h>
#include <signal.h>

// Forward declaration
void parseInput(Client* client);

void usage() {
  std::cout << "Usage: unittest_fault_tolerance [OPTIONS]" << std::endl;
  std::cout << "  -c [CONFIG] : Config JSON file (Default: " << CFG_FILE << ")" << std::endl; 
  std::cout << "  -C          : Run as client, defaults to running as server " << std::endl;
  std::cout << "  -v          : Increase verbosity" << std::endl;
  std::cout << "  -h          : Print this help text" << std::endl;
  std::cout << std::endl;
}

Server* server = NULL;
void signal_handler(int signum) {
  // Shutdown server on SIGINT
  if (server != NULL) {
    server->shutdownServer();
    server = NULL;
    exit(0);
  }
}

int main(int argc, char* argv[]) {

    int opt;
    bool isClient = false;

    Node* node;
    Client* client;

    while ((opt = getopt(argc, argv, "c:Cvh")) != -1) {
      switch(opt) {
        case 'c': CFG_FILE = optarg; break;
        case 'C': isClient = true; break;
        case 'v': LOG_LEVEL++; break;
        case 'h': usage(); return 0; break;
        default:  usage(); return 1; break;
      }
    }

    if (isClient) {
        node = new Client();
    } else {
        node = new Server();
    }

    node->initialize();

    if (isClient) {
        client = (Client*)node;
    } else {
        server = (Server*)node;
        signal(SIGINT, signal_handler);
    }

    if (isClient) {
      parseInput(client);
    }

    if(!isClient) {
      server->log_put<int, int>(1, 55);
    }

    if (!isClient) {
        while(true){ /* loop until killed */ }
    }

    return 0;
}

void parseInput(Client* client) {
    std::string verb;
    int key;
    uint64_t value;

    while (1) {
        std::cin >> verb;

        if (verb.compare("get")) {
            std::cin >> key;
            std::cout << client->get<int, uint64_t>(key);
        }
        else if (verb.compare("put")) {
            std::cin >> key >> value;
            std::cout << client->put<int, uint64_t>(key, value);
        }

        verb = "";
    }
}
