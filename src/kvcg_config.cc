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
#include <boost/functional/hash.hpp>
#include "kvcg_logging.h"
#include "kvcg_config.h"
#include "kvcg_errors.h"

namespace pt = boost::property_tree;

int KVCGConfig::parse_json_file(std::string filename) {
    int status = KVCG_ESUCCESS;
    int backupcnt = 0;
    LOG(DEBUG) << "Opening file: " << filename;

    pt::ptree root;
    try {
        pt::read_json(filename, root);
        for (pt::ptree::value_type &server : root.get_child("servers")) {
            std::string server_name = server.second.get<std::string>("name");
            LOG(DEBUG3) << "Parsing server: " << server_name;
            std::pair<int, int> keyRange = {server.second.get<int>("minKey"), server.second.get<int>("maxKey")};
            if (keyRange.first > keyRange.second) {
                LOG(ERROR) << "Invalid key range: [" << keyRange.first << ", " << keyRange.second << "]";
                status = KVCG_EBADCONFIG;
                goto exit;
            }
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

            // Make sure this key range does not overlap any previous
            for (auto& foundServer : serverList) {
                for (auto kr : foundServer->getPrimaryKeys()) {
                    if (keyRange.first <= kr.second && kr.first <= keyRange.second) {
                        LOG(ERROR) << server_name << 
                            " key range [" << keyRange.first << ", " << keyRange.second << "] overlaps " <<
                            foundServer->getName() << " keys [" << kr.first << ", " << kr.second << "]";
                        status = KVCG_EBADCONFIG;
                        goto exit;
                    }
                }
            }

            // Only allow primary to be defined once.
            if (primServer->getPrimaryKeys().size() == 0) {
              primServer->addKeyRange(keyRange);
            } else {
              LOG(ERROR) << "Multiple defintions for server '" << server_name << "'";
              status = KVCG_EBADCONFIG;
              goto exit;
            }

            BOOST_FOREACH(pt::ptree::value_type &backup, server.second.get_child("backups")) {
                std::string backupName = backup.second.data();
                if (backupName == server_name) {
                    LOG(ERROR) << "Server " << server_name << " may not back up itself";
                    status = KVCG_EBADCONFIG;
                    goto exit;
                }
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
        status = KVCG_EBADCONFIG;
        goto exit;
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

std::size_t KVCGConfig::get_checksum() {
    // Generate checksum representation of config file
    std::size_t seed = 0;
    for (auto s : serverList)
        boost::hash_combine(seed, s->getHash());

    LOG(DEBUG3) << "Config hash - " << seed;
    return seed;
}
