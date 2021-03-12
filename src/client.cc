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
#include  <faulttolerance/ft_networking.h>

int Client::initialize() {
    int status = KVCG_ESUCCESS;
    LOG(INFO) << "Initializing Client";

    KVCGConfig kvcg_config;
    if (status = kvcg_config.parse_json_file(CFG_FILE))
        LOG(INFO) << "Failed to parse config file";
        goto exit;
    
    this->serverList = kvcg_config.getServerList();

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
        if (status = kvcg_connect(&server->net_data, server->getName(), PORT))
            goto exit;
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}
