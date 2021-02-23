/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>

void Server::connHandle() {
  LOG(INFO) << "Handling connection";
  // TODO: Handle this 
}

void Server::listen() {
  LOG(INFO) << "Opening Server";
  // TODO: Open port (socket or otherwise)

  LOG(INFO) << "Waiting for connections...";
  while(true) {
    // TODO: accept connections and handle.
    // should be a blocking call here...
    while(true){}

    // launch handle thread
    std::thread connhandle_thread(&Server::connHandle, this);
    connhandle_thread.detach();
  }
}

int Server::initialize() {
    int status = 0;
    std::thread listen_thread;

    LOG(INFO) << "Initializing Server";

    if (status = parse_json_file(this))
        goto exit;


    // Log this server configuration
    LOG(INFO) << "Hostname: " << this->getName();
    LOG(INFO) << "Primary Keys:";
    for (auto keyRange : primaryKeys) {
        LOG(INFO) << "  [" << keyRange.first << ", " << keyRange.second << "]";
    }
    LOG(INFO) << "Backup Servers:";
    for (auto backup : backupServers) {
        LOG(INFO) << "  " << backup->getName();
    }
    LOG(INFO) << "Backing up primaries:";
    for (auto primary : primaryServers) {
        LOG(INFO) << "  " << primary->getName();
        for (auto keyRange : primary->getPrimaryKeys()) {
            LOG(DEBUG) << "    [" << keyRange.first << ", " << keyRange.second << "]";
        }
    }

    // TODO: Open connection with backups

    // Start listening for clients
    listen_thread = std::thread(&Server::listen, this);
    listen_thread.join();

exit:
    return status;
}

bool Server::addKeyRange(std::pair<int, int> keyRange) {
  // TODO: Validate input
  primaryKeys.push_back(keyRange);
  return true;
}

bool Server::addPrimaryServer(Server* s) {
  // TODO: Validate input
  primaryServers.push_back(s);
  return true;
}


bool Server::addBackupServer(Server* s) {
  // TODO: Validate input
  backupServers.push_back(s);
  return true;
}


bool Server::isPrimary(int key) {
    for (auto el : primaryKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}
