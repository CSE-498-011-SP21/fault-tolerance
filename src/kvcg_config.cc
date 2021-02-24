/****************************************************
 *
 * Config File Parser
 *
 ****************************************************/

#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include "fault_tolerance.h"
#include "kvcg_logging.h"
#include "kvcg_config.h"

namespace pt = boost::property_tree;

int KVCGConfig::parse_json_file(std::string filename) {
    int status = 0;
    int backupcnt = 0;
    LOG(DEBUG) << "Opening file: " << filename;

    pt::ptree root;
    try {
        pt::read_json(filename, root);
        for (pt::ptree::value_type &server : root.get_child("servers")) {
            std::string server_name = server.second.get<std::string>("name");
            LOG(DEBUG3) << "Parsing server: " << server_name;
            std::pair<int, int> keyRange = {server.second.get<int>("minKey"), server.second.get<int>("maxKey")};
            LOG(DEBUG3) << "Key range: " << keyRange.first << "-" << keyRange.second;
            Server* primServer = NULL;
            for (auto& foundServer : serverList) {
                if (foundServer->getName() == server_name) {
                    primServer = foundServer;
                    break;
                }
            }
            if (primServer == NULL) {
               primServer = new Server();
               primServer->setName(server_name);
               serverList.push_back(primServer);
            }
            primServer->addKeyRange(keyRange);
            BOOST_FOREACH(pt::ptree::value_type &backup, server.second.get_child("backups")) {
                std::string backupName = backup.second.data();
                // See if we have this server already
                Server* backupServer = NULL;
                for (auto& foundServer : serverList) {
                    if (foundServer->getName() == backupName) {
                        backupServer = foundServer;
                        break;
                    }
                }
                if (backupServer == NULL) {
                    backupServer = new Server();
                    backupServer->setName(backupName);
                    serverList.push_back(backupServer);
                }
                LOG(DEBUG3) <<"Adding backup: " << backupServer->getName();
                backupServer->addPrimaryServer(primServer);
                primServer->addBackupServer(backupServer);
            }
        }
    } catch (std::exception const& e) {
        LOG(ERROR) << "Failed reading " << filename << ": " << e.what();
        status = 1;
        goto exit;
    }

exit:
    return status;
}

