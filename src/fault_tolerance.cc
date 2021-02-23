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
#include <boost/asio/ip/host_name.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

namespace pt = boost::property_tree;

// set defaults
std::string CFG_FILE = "./kvcg.json";
int LOG_LEVEL = INFO;

const auto HOSTNAME = boost::asio::ip::host_name();

std::vector <Server*> foundServers;

int parse_json_file(Server* inServer) {
    int status = 0;
    int backupcnt = 0;
    LOG(DEBUG) << "Opening file: " << CFG_FILE;

    pt::ptree root;
    try {
        pt::read_json(CFG_FILE, root);
        for (pt::ptree::value_type &server : root.get_child("servers")) {
            std::string server_name = server.second.get<std::string>("name");
            LOG(DEBUG3) << "Parsing server: " << server_name;
            std::pair<int, int> keyRange = {server.second.get<int>("minKey"), server.second.get<int>("maxKey")};
            LOG(DEBUG3) << "Key range: " << keyRange.first << "-" << keyRange.second;
            Server* primServer = NULL;
            for (auto& foundServer : foundServers) {
                if (foundServer->getName() == server_name) {
                    primServer = foundServer;
                    break;
                }
            }
            if (primServer == NULL) {
               primServer = new Server();
               primServer->setName(server_name);
               foundServers.push_back(primServer);
            }
            primServer->addKeyRange(keyRange);
            BOOST_FOREACH(pt::ptree::value_type &backup, server.second.get_child("backups")) {
                std::string backupName = backup.second.data();
                // See if we have this server already
                Server* backupServer = NULL;
                for (auto& foundServer : foundServers) {
                    if (foundServer->getName() == backupName) {
                        backupServer = foundServer;
                        break;
                    }
                }
                if (backupServer == NULL) {
                    backupServer = new Server();
                    backupServer->setName(backupName);
                    foundServers.push_back(backupServer);
                }
                LOG(DEBUG3) <<"Adding backup: " << backupServer->getName();
                backupServer->addPrimaryServer(primServer);
                primServer->addBackupServer(backupServer);
            }
        }

        if (inServer) {
            // assign to server in foundServer list
            bool matched = false;
            for (auto& foundServer : foundServers) {
                if(foundServer->getName() == HOSTNAME) {
                    LOG(DEBUG3) << "Matched config server to " << HOSTNAME;
                    *inServer = *foundServer;
                    matched = true;
                }
            }
            if(!matched) {
                LOG(ERROR) << "Could not find server in config file: " << HOSTNAME;
                status = 1;
                goto exit;
            }
        }
    } catch (std::exception const& e) {
        LOG(ERROR) << "Failed reading " << CFG_FILE << ": " << e.what();
        status = 1;
        goto exit;
    }

exit:
    return status;
}

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
    LOG(INFO) << "Hostname: " << HOSTNAME;
    this->setName(HOSTNAME);

    if (status = parse_json_file(this))
        goto exit;


    // Log this server configuration
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

int Client::initialize() {
    int status = 0;
    LOG(INFO) << "Initializing Client";

    if (status = parse_json_file(NULL))
        goto exit;

exit:
    return status;
}
