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
                shard->addServer(backup);
            }

            this->shardList.push_back(shard);
        }
    }

exit:
    return status;
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
