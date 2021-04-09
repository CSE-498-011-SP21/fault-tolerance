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
    this->provider = kvcg_config.getProvider();

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


/**
   *
   * Get value in hash table on servers at key
   *
   * @param key - key to lookup in table
   *
   * @return value stored in table
   *
   */
  data_t Client::get(unsigned long long key) {
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

    cse498::unique_buf rawBuf(dataSize);
    rawBuf.cpyTo(rawData, dataSize);

    ft::Shard* shard = this->getShard(key);
    ft::Server* primary;

    if (shard == nullptr) {
        LOG(ERROR) << "Could not find shard object";
        status = 
        goto exit;
    }

    primry = shard->getPrimary();
    if (primary == nullptr) {
        LOG(ERROR) << "Could not find primary server object";
        goto exit;
    }

    if (!server->primary_conn->try_send(rawBuf, dataSize)) {
        // TODO: add some timeout/max tries?
        primary = getPrimaryOnFailure(key, shard);
    }

    primary->primary_conn->wait_send();
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
    std::vector<Server*> shardServers = shard->getServers();
    int status = KVCG_ESUCCESS;

    // TODO: connection request instead?
    RequestWrapper<unsigned long long, data_t*>* pkt = new RequestWrapper<unsigned long long, data_t*>();
    pkt->key = key;
    pkt->value = NULL;
    pkt->requestInteger = REQUEST_DISCOVER; // TOOD: this does not exist

    std::vector<char> serializeData = serialize(*pkt);
    char* rawData = &serializeData[0];
    size_t dataSize = serializeData.size();

    cse498::unique_buf rawBuf(dataSize);
    rawBuf.cpyTo(rawData, dataSize);
    
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
    // loop thru servers doing try-receive until we get one response
    while (check) {
        for (auto server : shardServers) {
            // no idea if this is right lol
            server->primary_conn->register_mr(rawBuf, // is this costly to do each time?
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    key);
            
            if(server->primary_conn->try_recv(rawBuf, size)) { // TODO: insert size
                check = false;
                break;
            }
        }
    }

    // TODO: extract response from buffer
    ft::Server* newPrimary;
    
    LOG(DEBUG) << "New primary is " << newPrimary->getName();

    shard->getPrimary()->alive = false;
    shard->setPrimary(newPrimary);
    return newPrimary;

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
  }
  // client_listen --> build out to work for discovery
  // packet with an initial bit to determine if it is a log request or discovery request
};