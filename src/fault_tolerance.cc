/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <sstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace pt = boost::property_tree;

// set defaults
std::string CFG_FILE = "./kvcg.json";
int LOG_LEVEL = INFO;

int parse_json_file() {
    int status = 0;
    LOG(DEBUG) << "Opening file: " << CFG_FILE;

    pt::ptree root;
    try {
        pt::read_json(CFG_FILE, root);
    } catch (const boost::property_tree::json_parser_error &e) {
        LOG(ERROR) << "Failed reading " << CFG_FILE << ": " << e.message().c_str();
        return 1;
    }
    // get fields with...
    // int threads = root.get<int>("threads", DEFAULT_NUM_THREADS);

    return status;
}

int init_server() {
    int status = 0;
    LOG(INFO) << "Initializing Server";

    if (status = parse_json_file()) {
        return status;
    }

    return status;
}

int init_client() {
    int status = 0;
    LOG(INFO) << "Initializing Client";

    if (status = parse_json_file()) {
      return status;
    }

    return status;
}
