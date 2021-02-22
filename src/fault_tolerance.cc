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
#include <boost/foreach.hpp>

namespace pt = boost::property_tree;

// set defaults
std::string CFG_FILE = "./kvcg.json";
int LOG_LEVEL = INFO;


int parse_json_file() {
    int status = 0;
    int backupcnt = 0;
    LOG(DEBUG) << "Opening file: " << CFG_FILE;

    pt::ptree root;
    try {
        pt::read_json(CFG_FILE, root);
        for (pt::ptree::value_type &server : root.get_child("servers")) {
            LOG(DEBUG) << server.second.get<std::string>("name");
            backupcnt = 0;
            BOOST_FOREACH(pt::ptree::value_type &backup, server.second.get_child("backups")) {
                LOG(DEBUG) << "  backup: " << backup.second.data();
                backupcnt++;
            }
            if (backupcnt != 1) {
                // TBD: For now only support exactly 1 backup
                LOG(ERROR) << "Only supporting 1 backup";
                status = 1;
                goto exit;
            }
        }
    } catch (std::exception const& e) {
        LOG(ERROR) << "Failed reading " << CFG_FILE << ": " << e.what();
        status = 1;
        goto exit;
    }

exit:
    return status;
}

int Server::initialize() {
    int status = 0;
    LOG(INFO) << "Initializing Server";

    if (status = parse_json_file())
        goto exit;

exit:
    return status;
}

int Client::initialize() {
    int status = 0;
    LOG(INFO) << "Initializing Client";

    if (status = parse_json_file())
        goto exit;

exit:
    return status;
}
