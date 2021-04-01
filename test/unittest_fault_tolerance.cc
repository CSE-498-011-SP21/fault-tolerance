/****************************************************
 *
 * Testing for Fault Tolerance API
 *
 ****************************************************/
#include <faulttolerance/fault_tolerance.h>
#include <chrono>
#include <string>
#include <stdio.h>
#include <iostream>
#include <getopt.h>
#include <signal.h>

namespace ft = cse498::faulttolerance;

// Forward declaration
void parseClientInput(ft::Client* client);
void parseServerInput(ft::Server* server);

void usage() {
  std::cout << "Usage: unittest_fault_tolerance [OPTIONS]" << std::endl;
  std::cout << "  -c [CONFIG] : Config JSON file (Default: ./kvcg.json)" << std::endl; 
  std::cout << "  -C          : Run as client, defaults to running as server " << std::endl;
  std::cout << "  -v          : Increase verbosity" << std::endl;
  std::cout << "  -h          : Print this help text" << std::endl;
  std::cout << std::endl;
}

ft::Server* server = NULL;
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

    ft::Node* node;
    ft::Client* client;

    std::string cfg_file = "./kvcg.json";

    while ((opt = getopt(argc, argv, "c:Cvh")) != -1) {
      switch(opt) {
        case 'c': cfg_file = optarg; break;
        case 'C': isClient = true; break;
        case 'v': LOG_LEVEL++; break;
        case 'h': usage(); return 0; break;
        default:  usage(); return 1; break;
      }
    }

    if (isClient) {
        node = new ft::Client();
    } else {
        node = new ft::Server();
    }

    if(status = node->initialize(cfg_file))
      goto exit;

    if (isClient) {
        client = (ft::Client*)node;
    } else {
        server = (ft::Server*)node;
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

void parseServerInput(ft::Server* server) {
    std::string cmd;
    unsigned long long key;
    data_t* value = new data_t();

    // 4076 is the maximum length of data we can send if we have a packet size of 4096
    value->data = new char[4076];

    while (true) {
        std::cout << "Command (p-print, l-log, q-quit): ";
        std::cin >> cmd;
        if (cmd == "p") {
          server->printServer(INFO);
        } else if (cmd == "l") {
            std::cout << "Enter Key (unsigned long long): ";
            std::cin >> key;
            std::cin.ignore();
            std::cout << "Enter Value (string): ";
            std::cin.getline(value->data, 4076);
            value->size = strlen(value->data);
            server->log_put(key, value);
        } else if (cmd == "q") {
          break;
        } else {
          std::cout << "Invalid command: " << cmd << std::endl;
        }

    }

    delete[] value->data;
}

void parseClientInput(ft::Client* client) {
    std::string cmd;
    unsigned long long key;
    data_t* value = new data_t();

    // 4076 is the maximum length of data we can send if we have a packet size of 4096
    value->data = new char[4076];

    while (true) {
        std::cout << "Command (g-get, p-put, q-quit): ";
        std::cin >> cmd;
        if (cmd == "g") {
          std::cout << "Enter Key (unsigned long long): ";
          std::cin >> key;
          value = client->get(key);
          std::cout << &value;
        } else if (cmd == "p") {
          std::cout << "Enter Key (unsigned long long): ";
          std::cin >> key;
          std::cin.ignore();
          std::cout << "Enter Value (string): ";
          std::cin.getline(value->data, 4076);
          value->size = strlen(value->data);
          std::cout << client->put(key, value);
        } else if (cmd == "q") {
          break;
        } else {
          std::cout << "Invalid command: " << cmd << std::endl; 
        }
    }

    delete[] value->data;
}
