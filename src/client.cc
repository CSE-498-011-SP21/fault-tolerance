/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/fault_tolerance.h>
#include <faulttolerance/client.h>
#include <faulttolerance/server.h>
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

namespace ft = cse498::faulttolerance;

int ft::Client::initialize(std::string cfg_file) {
    int status = KVCG_ESUCCESS;
    LOG(INFO) << "Initializing Client";

	KVCGConfig kvcg_config;
    if (status = kvcg_config.parse_json_file(cfg_file)) {
        LOG(INFO) << "Failed to parse config file";
        goto exit;
    }

    this->serverList = kvcg_config.getServerList();
    this->clientPort = kvcg_config.getClientPort();
    this->provider = kvcg_config.getProvider();


    LOG(DEBUG4) << "Iterate through servers: " << this->serverList.size();
    for (ft::Server* server : this->serverList) {
        for (std::pair<unsigned long long, unsigned long long> range : server->getPrimaryKeys()) {
            ft::Shard* shard = new ft::Shard(range);
            shard->addServer(server);
            shard->setPrimary(server);

            for (ft::Server* backup : server->getBackupServers()) {
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

int ft::Client::connect_servers() {
    LOG(INFO) << "Connecting to Servers";

    int status = KVCG_ESUCCESS;

    for (auto server: this->serverList) {
        LOG(DEBUG) << "  Connecting to " << server->getName() << " (addr: " << server->getAddr() << ")";
        cse498::unique_buf hello(6);
        hello.cpyTo("hello\0", 6);
        server->primary_conn = new cse498::Connection(server->getAddr().c_str(), false, this->clientPort, this->provider);
        // Initial send
        // server->primary_conn->send(hello, 6);
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int ft::Client::discoverPrimary(ft::Shard* shard) {
    int status = KVCG_ESUCCESS;
    bool found = false;
    int offset;
    size_t numRanges;
    unsigned long long minKey, maxKey;
    LOG(INFO) << "Discovering primary for shard [" << shard->getLowerBound() << ", " << shard->getUpperBound() << "]";

    // Try to establish connection to each server in shard (non-blocking, assume down if unable)
    for (auto server : shard->getServers()) {
        server->primary_conn = new cse498::Connection(server->getAddr().c_str(), false, this->clientPort, this->provider);
        if(!server->primary_conn->connect()) {
            // TBD: is connect() blocking? is there an alternative?
            continue;
        }
        cse498::unique_buf resp(4096);
        server->primary_conn->recv(resp, 4096);
        memcpy(&numRanges, resp.get(), sizeof(size_t)); 

        // TODO: Calculate if number of ranges creates buffer that is >4096 and handle
        offset = sizeof(size_t);
        for (int i=0; i < numRanges; i++) {
            memcpy(&minKey, resp.get()+offset, sizeof(unsigned long long));
            offset += sizeof(unsigned long long);
            memcpy(&maxKey, resp.get()+offset, sizeof(unsigned long long));
            offset += sizeof(unsigned long long);
            if (minKey == shard->getLowerBound() && maxKey == shard->getUpperBound()) {
                LOG(INFO) << "Found primary " << server->getName();
                shard->setPrimary(server);
                found = true;
                break;
            }
        }

        if (found) break;
    }

    if (!found) {
        LOG(ERROR) << "Could not find primary server for shard";
        status = KVCG_EUNKNOWN;
    }

    return status;
}

int ft::Client::put(unsigned long long key, data_t* value) {
    int status = KVCG_ESUCCESS;

    LOG(INFO) << "Sending PUT (" << key << "): " << value;
    RequestWrapper<unsigned long long, data_t*>* pkt = new RequestWrapper<unsigned long long, data_t*>();
    pkt->key = key;
    pkt->value= value;
    pkt->requestInteger = REQUEST_INSERT;
    std::vector<char> serializeData = serialize(*pkt);
    char* rawData = &serializeData[0];
    size_t dataSize = serializeData.size();

    LOG(DEBUG4) << "raw data: " << rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    cse498::unique_buf rawBuf(dataSize);
    rawBuf.cpyTo(rawData, dataSize);

    ft::Shard* shard = this->getShard(key);
    ft::Server* server;

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

data_t* ft::Client::get(unsigned long long key) {
    // 1. Generate packet to be sent

    // 2. Determine which server is primary

    // 3. Send packet to primary server

    // 4. If failed goto 2. Maybe only try a fixed number of times

    // 5. Return value

    return nullptr;
}

ft::Shard* ft::Client::getShard(unsigned long long key) {
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
