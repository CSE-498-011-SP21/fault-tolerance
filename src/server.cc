/*****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>
#include <string.h>
#include <boost/functional/hash.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <netdb.h>
#include "kvcg_config.h"

const auto HOSTNAME = boost::asio::ip::host_name();

KVCGConfig kvcg_config;


void Server::connHandle(int socket) {
  LOG(INFO) << "Handling connection";
  char buffer[1024] = {0};
  int r = read(socket, buffer, 1024);
  LOG(INFO) << "Read: " << buffer;
}

void Server::server_listen() {
  LOG(INFO) << "Waiting for Client requests...";
  while(true) {
    // FIXME: This is placeholder for network-layer
    int new_socket;
    int addrlen = sizeof(net_data.address);
    if ((new_socket = accept(net_data.server_fd, (struct sockaddr *)&net_data.address,
            (socklen_t*)&addrlen)) < 0) {
        perror("accept");
        exit(1);
    }
    // launch handle thread
    std::thread connhandle_thread(&Server::connHandle, this, new_socket);
    connhandle_thread.detach();
  }
}

int Server::log_put(int key, size_t valueSize, char* value) {
    // Send transaction to backups
    LOG(INFO) << "Logging PUT (" << key << "): " << value;
    BackupPacket pkt(key, valueSize, value);
    char* rawData = pkt.serialize();

    int dataSize = sizeof(key) + sizeof(valueSize) + valueSize;
    LOG(DEBUG4) << "raw data: " << rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    // TODO: Backup in parallel - dependent on network-layer
    // datagram support
    for (auto backup : backupServers) {
        LOG(DEBUG) << "Backing up to " << backup->getName();
        // send(backup->socket, rawData, dataSize, 0);
    }

    return true;
}

#define PORT 8080
int Server::open_client_endpoint() {
    // TODO: This will be reworked by network-layer
    LOG(INFO) << "Opening Socket for Clients";
    int opt = 1;

    net_data.server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (net_data.server_fd == 0) {
        perror("socket failure");
        return 1;
    }
    if (setsockopt(net_data.server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        return 1;
    }

    net_data.address.sin_family = AF_INET;
    net_data.address.sin_addr.s_addr = INADDR_ANY;
    net_data.address.sin_port = htons(PORT);

    if (bind(net_data.server_fd, (struct sockaddr*)&net_data.address, sizeof(net_data.address)) < 0) {
        perror("bind error");
        return 1;
    }

    if (listen(net_data.server_fd, 3) < 0) {
        perror("listen");
       return 1;
    }

    return 0;
}

int Server::open_backup_endpoints() {
    // TODO: This will be reworked by network-layer
    LOG(INFO) << "Opening Backup Sockets for other Primaries";
    int opt = 1;
    struct sockaddr_in address;
    int new_socket;
    int addrlen = sizeof(address);
    char buffer[1024] = {0};

    // Should use different ports, but this is tricky...
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == 0) {
        perror("socket failed");
        return 1;
    }
    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
            &opt, sizeof(opt))) {
        perror("setsockopt");
        return 1;
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT+1); // ugly

    if (bind(fd, (struct sockaddr*) &address, sizeof(address)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(fd, 3) < 0) {
        perror("listen");
        return 1;
    }

    std::size_t cksum = kvcg_config.get_checksum();

    for(int i=0; i < primaryServers.size(); i++) {
        if ((new_socket = accept(fd, (struct sockaddr *)&address,
                (socklen_t*)&addrlen))<0) {
            perror("accept");
            return 1;
        }
        int valread = read(new_socket, buffer, 1024);
        bool matched = false;
        for(auto pserv : primaryServers) {
            if (buffer == pserv->getName()) {
                LOG(DEBUG2) << "Connection from " << pserv->getName() << " - socket " << new_socket;
                pserv->net_data.socket = new_socket;
                matched = true;
                break;
            }
        }
        if (!matched) {
            LOG(ERROR) << "Received connection from unrecognized server";
            return 1;
        }
        // send config checksum
        std::string cksum_str = std::to_string(cksum);
        send(new_socket, cksum_str.c_str(), cksum_str.size(), 0);
        // wait for response
        char o_cksum[64];
        read(new_socket, o_cksum, cksum_str.size());
        if (std::stoul(cksum_str) == std::stoul(o_cksum)) {
            LOG(DEBUG2) << "Config checksum matches";
        } else {
            LOG(ERROR) << " Config checksum from " << buffer << " (" << std::stoul(o_cksum) << ") does not match local (" << std::stoul(cksum_str) << ")";
            return 1;
        }
    }

    return 0;
}

int Server::connect_backups() {
    // TODO: This will be reworked by network-layer
    LOG(INFO) << "Connecting to Backups";

    int sock;
    struct hostent *he;

    std::size_t cksum = kvcg_config.get_checksum();

    for (auto backup: backupServers) {
        LOG(DEBUG) << "  Connecting to " << backup->getName();
        bool connected = false;
        while (!connected) {
            struct sockaddr_in addr;
            if ((sock = socket(AF_INET, SOCK_STREAM, 0)) <0) {
                perror("socket");
                return 1;
            }

            addr.sin_family = AF_INET;
            addr.sin_port = htons(PORT+1); // still ugly
            if ((he = gethostbyname(backup->getName().c_str())) == NULL ) {
                perror("gethostbyname");
                return 1;
            }
            memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

            if(connect(sock, (struct sockaddr *)&addr, sizeof(addr)) <0) {
                close(sock); // retry
                LOG(DEBUG4) << "  Connection failed, retrying";
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            connected = true;
            // send my name to backup
            send(sock, this->getName().c_str(), this->getName().size(), 0);
            // backup should reply with config checksum
            std::string cksum_str = std::to_string(cksum);
            char o_cksum[64];
            read(sock, o_cksum, cksum_str.size());
            // unconditionally send ours back before checking
            send(sock, cksum_str.c_str(), cksum_str.size(), 0);
            if (std::stoul(cksum_str) == std::stoul(o_cksum)) {
                LOG(DEBUG2) << "Config checksum matches";
            } else {
                LOG(ERROR) << " Config checksum from " << backup->getName() << " (" << std::stoul(o_cksum) << ") does not match local (" << std::stoul(cksum_str) << ")";
                return 1;
            }
        }
    }

    return 0;
}

int Server::initialize() {
    int status = 0;
    bool matched = false;
    std::thread listen_thread, open_backup_eps_thread;

    LOG(INFO) << "Initializing Server";

    if (status = kvcg_config.parse_json_file(CFG_FILE))
        goto exit;

    // Get this server from config
    for (auto s : kvcg_config.serverList) {
        if (s->getName() == HOSTNAME) {
            *this = *s;
            matched = true;
            break;
        }
    }
    if (!matched) {
        LOG(ERROR) << "Failed find " << HOSTNAME << " in " << CFG_FILE;
        status = 1;
        goto exit;
    }


    // Log this server configuration
    LOG(INFO) << "Hostname: " << this->getName();
    LOG(INFO) << "Primary Keys:";
    for (auto keyRange : primaryKeys) {
        LOG(INFO) << "  [" << keyRange.first << ", " << keyRange.second << "]";
    }
    LOG(INFO) << "Backup Servers:";
    for (auto backup : backupServers) {
        LOG(INFO) << "  " << backup->getName();
    }
    LOG(INFO) << "Backing up primaries:";
    for (auto primary : primaryServers) {
        LOG(INFO) << "  " << primary->getName();
        for (auto keyRange : primary->getPrimaryKeys()) {
            LOG(DEBUG) << "    [" << keyRange.first << ", " << keyRange.second << "]";
        }
        LOG(DEBUG) << "    Other backups:";
        for (auto ob : primary->getBackupServers())
            LOG(DEBUG) << "      " << ob->getName();
    }

    // Open connection for other servers to backup here
    open_backup_eps_thread = std::thread(&Server::open_backup_endpoints, this);

    // Connect to this servers backups
    if (status = connect_backups())
        goto exit;

    open_backup_eps_thread.join();

    // Open connection for clients
    if (status = open_client_endpoint())
        goto exit;

    // Start listening for clients
    listen_thread = std::thread(&Server::server_listen, this);
    listen_thread.join();

exit:
    close(net_data.server_fd);
    open_backup_eps_thread.join();
    return status;
}

bool Server::addKeyRange(std::pair<int, int> keyRange) {
  // TODO: Validate input
  primaryKeys.push_back(keyRange);
  return true;
}

bool Server::addPrimaryServer(Server* s) {
  // TODO: Validate input
  primaryServers.push_back(s);
  return true;
}


bool Server::addBackupServer(Server* s) {
  // TODO: Validate input
  backupServers.push_back(s);
  return true;
}


bool Server::isPrimary(int key) {
    for (auto el : primaryKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}

std::size_t Server::getHash() {
    std::size_t seed = 0;

    // TBD: sort? Right now config files must have same ordering

    boost::hash_combine(seed, boost::hash_value(getName()));
    for (auto keyRange : primaryKeys) {
        boost::hash_combine(seed, boost::hash_value(keyRange.first));
        boost::hash_combine(seed, boost::hash_value(keyRange.second));
    }
    for (auto p : primaryServers) {
        boost::hash_combine(seed, boost::hash_value(p->getName()));
        for (auto kr : p->getPrimaryKeys()) {
            boost::hash_combine(seed, boost::hash_value(kr.first));
            boost::hash_combine(seed, boost::hash_value(kr.second));
        }
        for (auto otherb : p->getBackupServers()) {
            boost::hash_combine(seed, boost::hash_value(otherb->getName()));
        }
    }
    for (auto b : backupServers) {
        boost::hash_combine(seed, boost::hash_value(b->getName()));
    }
    LOG(DEBUG3) << "Hashed " << getName() << " - " << seed;
    return seed;
}

