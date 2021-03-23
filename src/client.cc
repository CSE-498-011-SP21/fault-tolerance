/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/client.h>
#include <faulttolerance/kvcg_config.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <string.h>
#include <sstream>
#include <boost/asio/ip/host_name.hpp>
#include <data_t.hh>
#include <RequestTypes.hh>
#include <RequestWrapper.hh>
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


    LOG(DEBUG4) << "Iterate through servers: " << this->serverList.size();
    for (Server* server : this->serverList) {
        LOG(DEBUG4) << "Iterate through servers: " << this->serverList.size();
        for (std::pair<unsigned long long, unsigned long long> range : server->getPrimaryKeys()) {
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

int Client::put(unsigned long long key, data_t* value) {
    // Send transaction to server
    int status = KVCG_ESUCCESS;
    char *buf = new char[4096];

    LOG(INFO) << "Sending PUT (" << key << "): " << value;
    RequestWrapper<unsigned long long, data_t*> request{key, value, REQUEST_INSERT};

    std::vector<char> serializeData = serialize(request);
    *(size_t *) buf = serializeData.size();

    LOG(DEBUG4) << "raw data: " << (void*)buf;
    LOG(DEBUG4) << "data size: " << serializeData.size();

    Shard* shard = this->getShard(key);
    Server* server;
    if (shard == nullptr) {
        LOG(ERROR) << "Could not find shard object";
        goto exit;
    }

    server = shard->getPrimary();
    if (server == nullptr) {
        LOG(ERROR) << "Could not find primary server object";
        goto exit;
    }

    server->primary_conn->wait_send(buf, sizeof(size_t));
    memcpy(buf, serializeData.data(), serializeData.size());
    server->primary_conn->wait_send(buf, serializeData.size());

exit:
    delete[] buf;
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

data_t* Client::get(unsigned long long key) {
    // 1. Generate packet to be sent

    // 2. Determine which server is primary

    // 3. Send packet to primary server

    // 4. If failed goto 2. Maybe only try a fixed number of times

    // 5. Return value

    return nullptr;
}

Shard* Client::getShard(unsigned long long key) {
    LOG(DEBUG4) << "iterating through shards: " << shardList.size();
    for (auto shard : this->shardList) {
        LOG(DEBUG4) << "checking shard [" << shard->getLowerBound() << ", " << shard->getUpperBound() << "]";
        if (shard->containsKey(key)) {
            LOG(DEBUG4) << "Key is within the range";
            return shard;
        }
    }
    return nullptr;
}