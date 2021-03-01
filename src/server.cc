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

void Server::client_listen() {
  LOG(INFO) << "Waiting for Client requests...";
  while(true) {
    // FIXME: This is placeholder for network-layer
    int new_socket;
    int addrlen = sizeof(net_data.address);
    if ((new_socket = accept(net_data.server_fd, (struct sockaddr *)&net_data.address,
            (socklen_t*)&addrlen)) < 0) {
        break;
    }
    // launch handle thread
    std::thread connhandle_thread(&Server::connHandle, this, new_socket);
    connhandle_thread.detach();
  }
}

void Server::primary_listen(Server* pserver) {
    LOG(INFO) << "Waiting for backup requests from " << pserver->getName();

    int r;
    char buffer[1024] = {0};

    bool remote_closed = false;

    while(true) {
      r = read(pserver->net_data.socket, buffer, 1024);
      if (r > 0) {
        LOG(DEBUG3) << "Read " << r << " bytes from " << pserver->getName() << ": " << buffer;
        // FIXME: Server class probably needs to be templated altogether...
        //        for testing, assume key/values are ints...
        BackupPacket<int, int> pkt(buffer);
        LOG(INFO) << "Received from " << pserver->getName() << " (" << pkt.getKey() << "," << pkt.getValue() << ")";

        // TODO: Store it somewhere...

      } else if (r == 0) {
        // primary disconnected closed
        // TODO: On closing the socket fd, read can return 0.
        //       prints a confusing message on shutdown
        remote_closed = true;
        break;
      } else if (r < 0) {
        // Assume connection closed from our end
        LOG(DEBUG4) << "Closed connection to " << pserver->getName();
        break;
      }
    }

    if (remote_closed) {
        LOG(WARNING) << "Primary server " << pserver->getName() << " disconnected";
        if (HOSTNAME == pserver->getBackupServers()[0]->getName()) {
            LOG(INFO) << "Taking over as new primary";
            // FIXME: Implement - commit log and update backups
        } else {
            LOG(INFO) << "Not taking over as new primary";
            // FIXME: Implement - wait for update from new primary
        }
    }

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
    int i = 0;
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

    for(i=0; i < primaryServers.size(); i++) {
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
            // Received connection from a server not expected to be someone we are backing up.
            // it could be that the primary failed and one of its backups has taken over.
            LOG(DEBUG2) << "Connection from server that is not defined as primary";
            for(i=0; i < primaryServers.size(); i++) {
                for (auto pbackup : primaryServers[i]->getBackupServers()) {
                    if (buffer == pbackup->getName()) {
                        LOG(DEBUG2) << "Connection from primary server " << primaryServers[i]->getName() << " backup " << pbackup->getName();
                        matched = true;
                        // Store this backup as the new primary for the key range
                        pbackup->net_data.socket = new_socket;
                        primaryServers.push_back(pbackup);
                        // Remove the original primary from servers we are backing up
                        primaryServers.erase(primaryServers.begin()+i);
                        break;
                    }
                }
                if (matched) break;
            }
        }
        if (!matched) {
            LOG(ERROR) << "Received connection from unrecognized server";
            return 1;
        }
        // send config checksum
        std::string cksum_str = std::to_string(cksum);
        if(send(new_socket, cksum_str.c_str(), cksum_str.size(), 0) < 0) {
            LOG(ERROR) << "Failed sending checksum"; 
            return 1;
        }
        // Also send byte to indicate running as a backup
        std::string state = "b";
        if(send(new_socket, state.c_str(), state.size(), 0) < 0) {
            LOG(ERROR) << "Failed sending backup state indicator";
            return 1;
        }
        // wait for response
        char o_cksum[64];
        if(read(new_socket, o_cksum, cksum_str.size()) < 0) {
            LOG(ERROR) << "Failed to read checksum response";
            return 1;
        }
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
        }

        // send my name to backup
        if(send(sock, this->getName().c_str(), this->getName().size(), 0) < 0) {
            LOG(ERROR) << "Failed sending name";
            return 1;
        }
        // backup should reply with config checksum and its state
        std::string cksum_str = std::to_string(cksum);
        char o_cksum[64];
        if(read(sock, o_cksum, cksum_str.size()) < 0) {
            LOG(ERROR) << "Failed to read checksum response";
            return 1;
        }
        char o_state[1];
        if(read(sock, o_state, 1) < 0) {
            LOG(ERROR) << "Failed to read state";
            return 1;
        }
        // unconditionally send ours back before checking
        if(send(sock, cksum_str.c_str(), cksum_str.size(), 0) < 0) {
            LOG(ERROR) << "Failed sending checksum";
            return 1;
        }
        if (std::stoul(cksum_str) == std::stoul(o_cksum)) {
            LOG(DEBUG2) << "Config checksum matches";
        } else {
            LOG(ERROR) << " Config checksum from " << backup->getName() << " (" << std::stoul(o_cksum) << ") does not match local (" << std::stoul(cksum_str) << ")";
            return 1;
        }

        backup->net_data.socket = sock;

        // Right now, this server expects to be running as primary, and backup as secondary
        // There is a chance that this server previously failed and the backup took over.
        // See if backup is running as primary now. If so, this server is now backing it up instead.
        if (o_state[0] == 'p') {
            // Backup took over at some point. Become a backup to it now.
            // TODO: Verify only one backup server responds with this
            LOG(INFO) << "Backup Server " << backup->getName() << " took over as primary";
            primaryServers.push_back(backup);
            // Not a primary anymore, nobody backing this server up.
            backupServers.clear();
            break;
        } else if (o_state[0] != 'b') {
            LOG(ERROR) << "Could not determine state of " << backup->getName() << " - " << o_state;
            return 1;
        } else {
            LOG(DEBUG3) << backup->getName() << " is still running as backup";
        }
    }

    return 0;
}

int Server::initialize() {
    int status = 0;
    bool matched = false;
    std::thread open_backup_eps_thread;

    LOG(INFO) << "Initializing Server";

    if (status = kvcg_config.parse_json_file(CFG_FILE))
        goto exit;

    // Get this server from config
    for (auto s : kvcg_config.getServerList()) {
        if (s->getName() == HOSTNAME) {
            *this = *s;
            matched = true;
            break;
        }
    }
    if (!matched) {
        LOG(ERROR) << "Failed to find " << HOSTNAME << " in " << CFG_FILE;
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

    // Start listening for backup requests
    for (auto primary : primaryServers) {
        // TODO: a primary could fail and another backup
        //       could take over, but we do not have a connection to it.
        primary_listen_threads.push_back(new std::thread(&Server::primary_listen, this, primary));
    }

    // Open connection for clients
    if (status = open_client_endpoint())
        goto exit;

    // Start listening for clients
    client_listen_thread = new std::thread(&Server::client_listen, this);

exit:
    if (status)
      shutdownServer(); // shutdown on error
    if (open_backup_eps_thread.joinable())
        open_backup_eps_thread.join();

    return status;
}

void Server::shutdownServer() {
  LOG(INFO) << "Shutting down server";
  if (net_data.server_fd) {
    LOG(DEBUG3) << "Closing server fd - " << net_data.server_fd;
    shutdown(net_data.server_fd, SHUT_RDWR);
    close(net_data.server_fd);
  }
  if (client_listen_thread != nullptr && client_listen_thread->joinable()) {
    LOG(DEBUG3) << "Closing client thread";
    client_listen_thread->join();
  }
  for (auto b : backupServers) {
    if (b->net_data.socket) {
      LOG(DEBUG3) << "Closing socket to backup " << b->getName() << " - " << b->net_data.socket;
      shutdown(b->net_data.socket, SHUT_RDWR);
      close(b->net_data.socket);
    }
  }
  LOG(DEBUG3) << "Closing primary listening threads";
  for (auto& t : primary_listen_threads) {
    if (t->joinable())
      t->join();
  }
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

