/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include "kvcg_config.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>

int Client::initialize() {
    int status = 0;
    LOG(INFO) << "Initializing Client";

    KVCGConfig kvcg_config;
    if (status = kvcg_config.parse_json_file(CFG_FILE))
        goto exit;
exit:
    return status;
}

