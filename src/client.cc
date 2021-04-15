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
        /*
            // add rawBuf as client attribute
            // rawBuf mr key
            // rawbuf is some memory region on client --> use a client attribute
            // one rawBuf for client assuming we send the same data to each server (?)
            server->primary_conn->register_mr(rawBuf, // is this costly to do each time?
                        FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                        );
        */
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

// TODO: add getPrimaryOnFailure() to this in case of failure
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

    this->rawBuf.cpyTo(rawData, dataSize);

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

    if (!server->primary_conn->try_send(this->rawBuf, dataSize)) {
        server = getPrimaryOnFailure(key, shard);
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

/**
 *
 * Get value in hash table on servers at key
 *
 * @param key - key to lookup in table
 *
 * @return value stored in table
 *
 */
data_t* ft::Client::get(unsigned long long key) {
    int status = KVCG_ESUCCESS;

    // generate packet to be sent
    RequestWrapper<unsigned long long, data_t*>* pkt = new RequestWrapper<unsigned long long, data_t*>();
    pkt->key = key;
    pkt->value = NULL;
    pkt->requestInteger = REQUEST_GET;

    std::vector<char> serializeData = serialize(*pkt);
    char* rawData = &serializeData[0];
    size_t dataSize = serializeData.size();

    LOG(DEBUG4) << "raw data: " << rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    this->rawBuf.cpyTo(rawData, dataSize);

    ft::Shard* shard = this->getShard(key);
    ft::Server* primary;

    if (shard == nullptr) {
        LOG(ERROR) << "Could not find shard object";
        status = 
        goto exit;
    }

    primary = shard->getPrimary();
    if (primary == nullptr) {
        LOG(ERROR) << "Could not find primary server object";
        goto exit;
    }

    if (!primary->primary_conn->try_send(rawBuf, dataSize)) {
        // TODO: add some timeout/max tries?
        primary = getPrimaryOnFailure(key, shard);
    }

    primary->primary_conn->wait_send();

    // TODO: how to correctly receive response / need to use register_mr??
    //cse498::mr_t mr;
    //primary->primary_conn->registerMR(buf, 4096, mr); // ?? TODO: instead call rawBuf
    //cse498::free_mr(mr); // this would go further down, before returning
    primary->primary_conn->recv(rawBuf, 4096); // should this be a new buffer? size?

    Response* res = deserialize<Response>(rawBuf);

    return res->result;

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
  }


/**
 *
 * Get the shard storing the key
 *
 * @param key - key whose shard to search for
 *
 * @return shard storing key
 *
 */
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

/**
 *
 * Get the new primary server storing a key by broadcasting to the servers in a shard
 *
 * @param shard - shard whose servers to broadcast to
 *
 * @return new primary server storing the key
 *
 */
Server* Client::getPrimaryOnFailure(unsigned long long key, Shard* shard) {
    // NOTE: mainly confused on buffering and when I need to register_mr

    std::vector<Server*> shardServers = shard->getServers();
    int status = KVCG_ESUCCESS;

    RequestWrapper<unsigned long long, data_t*>* pkt = new RequestWrapper<unsigned long long, data_t*>();
    pkt->key = key;
    pkt->value = NULL;
    pkt->requestInteger = REQUEST_DISCOVER; // TOOD: this does not exist

    std::vector<char> serializeData = serialize(*pkt);
    char* rawData = &serializeData[0];
    size_t dataSize = serializeData.size();

    //cse498::unique_buf rawBuf(dataSize); // bc made rawBuf a field of the client
    this->rawBuf.cpyTo(rawData, dataSize);
    
    for (auto server : shardServers) {
      // TODO: what if not alive? -> don't want to check if alive bc the client may have it wrong
      // but what will async_send return if a server is dead? will it get stuck in the next loop waiting?
      LOG(DEBUG) << "Discovering " << server->getName();
      server->primary_conn->async_send(rawData, dataSize);
    }

    for (auto server : shardServers) {
        LOG(DEBUG2) << "Waiting for discovery to complete on " << server->getName();
        server->primary_conn->wait_send();
    }

    bool check = true;
    ft::Server* newPrimary;
    // loop thru servers doing try-receive until we get one response
    while (check) {
        for (auto server : shardServers) {
            if (server->primary_conn->try_recv(this->rawBuf, 1)) { // TODO: size? what are we receiving
                check = false;
                newPrimary = server; // only care which server responds, not what they respond with
                break;
            }
        }
    }
    
    LOG(DEBUG) << "New primary is " << newPrimary->getName();

    shard->getPrimary()->alive = false;
    shard->setPrimary(newPrimary);
    return newPrimary;

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}
};
