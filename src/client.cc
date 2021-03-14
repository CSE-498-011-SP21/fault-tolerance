/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/client.h>
#include <faulttolerance/kvcg_config.h>


#include <kvcg_logging.h>
#include <kvcg_errors.h>
// #include <iostream>
// #include <chrono>
// #include <thread>
// #include <string.h>
// #include <sstream>
#include <boost/asio/ip/host_name.hpp>
#include <kvcg_errors.h>
#include <faulttolerance/definitions.h>
#include <faulttolerance/shard.h>

int Client::initialize() {
//     int status = KVCG_ESUCCESS;
//     LOG(INFO) << "Initializing Client";

//     KVCGConfig kvcg_config;
//     if (status = kvcg_config.parse_json_file(CFG_FILE))
//         LOG(INFO) << "Failed to parse config file";
//         goto exit;
    
//     this->serverList = kvcg_config.getServerList();

//     if (status = this->connect_servers()) {
//         LOG(INFO) << "Failed to connect to servers";
//         goto exit;
//     }

// exit:
//     return status;

    return 0;
}

int Client::connect_servers() {
//     LOG(INFO) << "Connecting to Servers";

//     int status = KVCG_ESUCCESS;

//     for (auto server: this->serverList) {
//         LOG(DEBUG) << "  Connecting to " << server->getName();
//         if (status = kvcg_connect(&server->net_data, server->getName(), PORT))
//             goto exit;
//     }

// exit:
//     LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
//     return status;
    return 0;
}

int Client::put(key_t key, data_t value) {
    // Send transaction to server
    int status = KVCG_ESUCCESS;

    LOG(INFO) << "Sending PUT (" << key << "): " << value;

    Shard* shard = this->getShard(key);
    LOG(DEBUG4) << "Mapped key to shard";

    status = shard->put(key, value);

    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

data_t Client::get(key_t key) {
    // Send transaction to server
    int status = KVCG_ESUCCESS;

    // LOG(INFO) << "Sending GET (" << key << ")";

    Shard* shard = this->getShard(key);
    // LOG(DEBUG4) << "Mapped key to shard";

    data_t* value = new data_t();

    status = shard->get(key, value);

    // LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return *value;
}

Shard* Client::getShard(key_t key) {
    for (auto shard : this->shard_list) {
      if (shard->containsKey(key)) {
        return shard;
      }
    }
    return nullptr;
}
