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
#include <kvcg_logging.h>
#include <faulttolerance/kvcg_config.h>
#include <kvcg_errors.h>

namespace pt = boost::property_tree;
namespace ft = cse498::faulttolerance;

int KVCGConfig::parse_json_file(std::string filename) {
    int status = KVCG_ESUCCESS;
    int backupcnt = 0;
    bool hasKeys = false;
    LOG(DEBUG) << "Opening file: " << filename;

    pt::ptree root;
    try {
        pt::read_json(filename, root);

        std::string provider_str = root.get<std::string>("provider", "");
        if (provider_str == "sockets") {
            provider = cse498::Sockets;
        } else if (provider_str == "verbs") {
            provider = cse498::Verbs;
        } else if (provider_str == "") {
            LOG(DEBUG) << "Provider not specified - defaulting to sockets";
            provider = cse498::Sockets;
        } else {
            LOG(ERROR) << "Invalid provider (" << provider_str << "). Must be 'verbs' or 'sockets'";
            status = KVCG_EBADCONFIG;
            goto exit;
        }


        for (pt::ptree::value_type &server : root.get_child("servers")) {
            std::string server_name = server.second.get<std::string>("name");
            LOG(DEBUG3) << "Parsing server: " << server_name;
            
            std::string server_addr = server.second.get<std::string>("address", "");
            if (server_addr == "") {
                LOG(DEBUG3) << "Defaulting " << server_name << " address to name";
                server_addr = server_name;
            }
            std::pair<unsigned long long, unsigned long long> keyRange = {server.second.get<unsigned long long>("minKey", -1), server.second.get<unsigned long long>("maxKey", -1)};
            if (keyRange.first == -1 && keyRange.second == -1) {
                // No Key range specified. This is a backup only server.
                hasKeys = false;
            } else if (keyRange.first > keyRange.second) {
                LOG(ERROR) << "Invalid key range: [" << keyRange.first << ", " << keyRange.second << "]";
                status = KVCG_EBADCONFIG;
                goto exit;
            } else {
                LOG(DEBUG3) << "Key range: " << keyRange.first << "-" << keyRange.second;
                hasKeys = true;
            }
            ft::Server* primServer = NULL;
            for (auto& foundServer : serverList) {
                if (foundServer->getName() == server_name) {
                    primServer = foundServer;
                    break;
                }
            }
            if (primServer == NULL) {
               primServer = new ft::Server();
               primServer->setName(server_name);
               serverList.push_back(primServer);
            }

            // Only allow one definition
            if (primServer->getAddr() != "") {
                LOG(ERROR) << "Multiple definitions for server '" << server_name << "'";
                status = KVCG_EBADCONFIG;
                goto exit;
            }
            primServer->setAddr(server_addr);

            // Make sure this key range does not overlap any previous
            for (auto& foundServer : serverList) {
                for (auto kr : foundServer->getPrimaryKeys()) {
                    if (hasKeys && keyRange.first <= kr.second && kr.first <= keyRange.second) {
                        LOG(ERROR) << server_name << 
                            " key range [" << keyRange.first << ", " << keyRange.second << "] overlaps " <<
                            foundServer->getName() << " keys [" << kr.first << ", " << kr.second << "]";
                        status = KVCG_EBADCONFIG;
                        goto exit;
                    }
                }
            }

            if (hasKeys) {
              primServer->addKeyRange(keyRange);
              BOOST_FOREACH(pt::ptree::value_type &backup, server.second.get_child("backups")) {
                std::string backupName = backup.second.data();
                if (backupName == server_name) {
                    LOG(ERROR) << "Server " << server_name << " may not back up itself";
                    status = KVCG_EBADCONFIG;
                    goto exit;
                }
                // See if we have this server already
                ft::Server* backupServer = NULL;
                for (auto& foundServer : serverList) {
                    if (foundServer->getName() == backupName) {
                        backupServer = foundServer;
                        break;
                    }
                }
                if (backupServer == NULL) {
                    backupServer = new ft::Server();
                    backupServer->setName(backupName);
                    serverList.push_back(backupServer);
                }
                LOG(DEBUG3) <<"Adding backup: " << backupServer->getName();
                backupServer->addPrimaryServer(primServer);
                primServer->addBackupServer(backupServer);
              }
           }
        }

        // Default address of any remaining backups
        for (auto& foundServer : serverList) {
            if (foundServer->getAddr() == "") {
                LOG(DEBUG3) << "Defaulting " << foundServer->getName() << " address to name";
                foundServer->setAddr(foundServer->getName());
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

    boost::hash_combine(seed, provider);

    LOG(DEBUG3) << "Config hash - " << seed;
    return seed;
}
