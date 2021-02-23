/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>

int Client::initialize() {
    int status = 0;
    LOG(INFO) << "Initializing Client";

    if (status = parse_json_file(NULL))
        goto exit;

exit:
    return status;
}
