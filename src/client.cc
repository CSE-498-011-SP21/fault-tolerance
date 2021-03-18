/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/fault_tolerance.h>
#include <faulttolerance/kvcg_config.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <string.h>
#include <sstream>
#include <boost/asio/ip/host_name.hpp>
#include <kvcg_errors.h>
#include <networklayer/connection.hh>


int Client::initialize() {
    int status = KVCG_ESUCCESS;
    LOG(INFO) << "Initializing Client";

    KVCGConfig kvcg_config;
    if (status = kvcg_config.parse_json_file(CFG_FILE))
        LOG(INFO) << "Failed to parse config file";
        goto exit;
    
    this->serverList = kvcg_config.getServerList();

    for (Server* server : this->serverList) {
        for (std::pair<int, int> range : server->getPrimaryKeys()) {
            Shard* shard = new Shard(range);
            shard->addServer(server);
            shard->setPrimary(server);

            for (Server* backup : server->getBackupServers()) {
                if (backup->isBackup(range.first)) {
                    shard->addServer(backup);
                }
            }

            this->shardList.push_back(shard);
        }
    }

    if (status = this->connect_servers()) {
        LOG(INFO) << "Failed to connect to servers";
        goto exit;
    }

exit:
    return status;
}

int Client::connect_servers() {
    LOG(INFO) << "Connecting to Servers";

    int status = KVCG_ESUCCESS;

    for (auto server: this->serverList) {
        LOG(DEBUG) << "  Connecting to " << server->getName();
        std::string hello = "hello\0";
        server->primary_conn = new cse498::Connection(server->getName().c_str());
        // Initial send
        server->primary_conn->wait_send(hello.c_str(), hello.length()+1);
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}
