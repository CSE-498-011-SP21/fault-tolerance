/****************************************************
 *
 * Testing for Fault Tolerance API
 *
 ****************************************************/
#include <faulttolerance/fault_tolerance.h>
#include <chrono>
#include <stdio.h>
#include <iostream>
#include <getopt.h>
#include <signal.h>

// Forward declaration
void parseClientInput(Client* client);
void parseServerInput(Server* server);

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
    int status;
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

    if(status = node->initialize())
      goto exit;

    if (isClient) {
        client = (Client*)node;
    } else {
        server = (Server*)node;
        signal(SIGINT, signal_handler);
    }


    // Running as server or client, prompt for commands
    if (isClient) {
        parseClientInput(client);
    } else {
        parseServerInput(server);
        server->shutdownServer();
    }

exit:
    return status;
}

void parseServerInput(Server* server) {
    std::string cmd;
    int key, value;

    while (true) {
        std::cout << "Command (p-print, l-log, q-quit): ";
        std::cin >> cmd;
        if (cmd == "p") {
          server->printServer(INFO);
        } else if (cmd == "l") {
            std::cout << "Enter Key (int): ";
            std::cin >> key;
            std::cout << "Enter Value (int): ";
            std::cin >> value;
            server->log_put<int, int>(key, value);
        } else if (cmd == "q") {
          return;
        } else {
          std::cout << "Invalid command: " << cmd << std::endl;
        }

    }
}

void parseClientInput(Client* client) {
    std::string cmd;
    unsigned long long key;
    data_t* value = new data_t();
    int temp_value;

    while (true) {
        std::cout << "Command (g-get, p-put, q-quit): ";
        std::cin >> cmd;
        if (cmd == "g") {
          std::cout << "Enter Key (int): ";
          std::cin >> key;
          value = client->get(key);
          std::cout << &value;
        } else if (cmd == "p") {
          std::cout << "Enter Key (int): ";
          std::cin >> key;
          std::cout << "Enter Value (int): ";
          std::cin >> temp_value;
          value->size = 4;
          value->data = (char*)(&temp_value);
          std::cout << client->put(key, value);
        } else if (cmd == "q") {
          return;
        } else {
          std::cout << "Invalid command: " << cmd << std::endl; 
        }
    }
}
