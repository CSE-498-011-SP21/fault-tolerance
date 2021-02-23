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

int parse_json_file(Server* inServer) {
    int status = 0;
    int backupcnt = 0;
    LOG(DEBUG) << "Opening file: " << CFG_FILE;
    std::vector <Server*> foundServers;

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

BackupPacket::BackupPacket(char* rawData) {
  // Process raw bytes into a struct
  memcpy(&this->key, rawData, sizeof(this->key));
  memcpy(&this->valueSize, rawData+sizeof(this->key), sizeof(this->valueSize));
  memcpy(&this->value, rawData+sizeof(this->key)+sizeof(this->valueSize), this->valueSize);
}

BackupPacket::BackupPacket(int key, size_t valueSize, char* value) {
    this->key = key;
    this->valueSize = valueSize;
    this->value = value;
}

char* BackupPacket::serialize() {
    char* rawData = (char*) malloc(sizeof(key)+sizeof(valueSize)+valueSize);
    memcpy(rawData, &this->key, sizeof(key));
    memcpy(rawData+sizeof(key), &this->valueSize, sizeof(valueSize));
    memcpy(rawData+sizeof(key)+sizeof(valueSize), value, valueSize);
    return rawData;
}

