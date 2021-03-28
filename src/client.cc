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
    if (status = kvcg_config.parse_json_file(CFG_FILE)) {
        LOG(INFO) << "Failed to parse config file";
        goto exit;
    }

    this->serverList = kvcg_config.getServerList();
    this->provider = kvcg_config.getProvider();


    LOG(DEBUG4) << "Iterate through servers: " << this->serverList.size();
    for (Server* server : this->serverList) {
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
        LOG(DEBUG) << "  Connecting to " << server->getName() << " (addr: " << server->getAddr() << ")";
        cse498::unique_buf hello(6);
        hello.cpyTo("hello\0", 6);
        server->primary_conn = new cse498::Connection(server->getAddr().c_str(), false, CLIENT_PORT, provider);
        // Initial send
        server->primary_conn->send(hello, 6);
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int Client::put(unsigned long long key, data_t* value) {
    int status = KVCG_ESUCCESS;

    LOG(INFO) << "Sending PUT (" << key << "): " << value;
    RequestWrapper<unsigned long long, data_t*> pkt{key, value, REQUEST_INSERT};
    std::vector<char> serializeData = serialize(pkt);
    char* rawData = &serializeData[0];
    size_t dataSize = serializeData.size();

    LOG(DEBUG4) << "raw data: " << (void*)rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    cse498::unique_buf rawBuf(dataSize);
    rawBuf.cpyTo(rawData, dataSize);

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

    server->primary_conn->send(rawBuf, dataSize);

exit:
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