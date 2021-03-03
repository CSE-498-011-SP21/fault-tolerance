/*****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <assert.h>
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
size_t cksum;

bool shutting_down = false;

void Server::connHandle(int socket) {
  LOG(INFO) << "Handling connection";
  char buffer[1024] = {'\0'};
  int r = read(socket, buffer, 1024);
  LOG(INFO) << "Read: " << (void*) buffer;
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

    int r;
    char buffer[64] = {'\0'};
    Server* primServer = pserver; // May change over time
    bool remote_closed = false;

    while(true) {
        LOG(INFO) << "Waiting for backup requests from " << primServer->getName();
        remote_closed = false;
        while(true) {
          r = read(primServer->net_data.socket, buffer, 64);
          if (r > 0) {
            LOG(DEBUG3) << "Read " << r << " bytes from " << primServer->getName() << ": " << (void*) buffer;
            // FIXME: Server class probably needs to be templated altogether...
            //        for testing, assume key/values are ints...
            BackupPacket<int, int> pkt(buffer);
            LOG(INFO) << "Received from " << primServer->getName() << " (" << pkt.getKey() << "," << pkt.getValue() << ")";

            // TODO: Store it somewhere...

          } else if (r == 0) {
            // primary disconnected closed
            // TODO: On closing the socket fd, read can return 0.
            //       prints a confusing message on shutdown
            remote_closed = true;
            break;
          } else if (r < 0) {
            // Assume connection closed from our end
            LOG(DEBUG4) << "Closed connection to " << primServer->getName();
            return;
          }
        }

        if (shutting_down)
            return;

        if (remote_closed) {
            LOG(WARNING) << "Primary server " << primServer->getName() << " disconnected";

            // remove from list of primaries
            std::vector<Server*> newPrimaries;
            for (auto p : primaryServers) {
               if (p->getName() != primServer->getName())
                 newPrimaries.push_back(p);
            }
            Server* newPrimary = NULL;
            for (auto s : primServer->getBackupServers()) {
                // first alive server takes over
                if (s->alive) {
                    newPrimary = s;
                    break;
                }
            }

            // If we are still running, then there still had to be a backup...
            assert(newPrimary != NULL);

            // rotate vector of backups for who is next in line
            newPrimary->clearBackupServers();
            for (int i=1; i< primServer->getBackupServers().size(); i++) {
                newPrimary->addBackupServer(primServer->getBackupServers()[i]);
            }
            // Keep old primary on list of backups, but mark it as down
            primServer->alive = false;
            newPrimary->addBackupServer(primServer);

            // Log
            LOG(DEBUG) << "New primary: "<< newPrimary->getName();
            LOG(DEBUG2) << "  Backups:";
            for (auto newBackup : newPrimary->getBackupServers()) {
                if(newBackup->alive)
                  LOG(DEBUG2) << "    " << newBackup->getName();
                else
                  LOG(DEBUG2) << "    " << newBackup->getName() << " (down)";
            }

            if (HOSTNAME == newPrimary->getName()) {
                LOG(INFO) << "Taking over as new primary";
                // TODO: Commit log
                // TODO: Verify backups match

                // become primary for old primary's keys
                for (auto kr : primServer->primaryKeys)
                  addKeyRange(kr);

                // Add new backups to my backups
                for (auto newBackup : newPrimary->getBackupServers()) {
                    bool exists = false;
                    for (auto existingBackup : backupServers) {
                        if (existingBackup->getName() == newBackup->getName()) {
                            // this server is already backing us up for our primary keys
                            // add another key range to it
                            for (auto kr : primServer->primaryKeys)
                                existingBackup->backupKeys.push_back(kr);
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        // Someone new is backing us up now
                        addBackupServer(newBackup);
                        for (auto kr : primServer->primaryKeys)
                            newBackup->backupKeys.push_back(kr);
                        // Connect to new backup
                        connect_backups(newBackup);
                    }
                }

                printServer(DEBUG);

                // When the old primary comes back up, it will call connect_backups()
                // Be ready to accept it and reply 'p' for state
                std::thread t = std::thread(&Server::open_backup_endpoints, this, std::ref(primServer), 'p');
                t.detach(); // it may or may not come back online

                // This thread can exit, not listening anymore from old primary
                return;

            } else {
                LOG(INFO) << "Not taking over as new primary";


                // temporarily set primaryServers to the one we need to connect with
                primaryServers.clear();
                primaryServers.push_back(newPrimary);
                // blocks until new primary connects
                open_backup_endpoints(newPrimary, 'b');
                // copy back vector of primaries
                newPrimaries.push_back(newPrimary);
                primaryServers = std::move(newPrimaries);

                // resume this primary_listen thread
                primServer = newPrimary;
            }
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

int Server::open_backup_endpoints(Server* primServer /* NULL */, char state /*'b'*/) {
    // TODO: This will be reworked by network-layer
    if (primServer == NULL)
        LOG(INFO) << "Opening Backup Sockets for other Primaries";
    else {
        LOG(INFO) << "Opening Socket for " << primServer->getName();
        if(primServer->net_data.socket) {
            close(primServer->net_data.socket);
        }
    }
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

    int numConns = primaryServers.size();
    if (primServer != NULL) {
        numConns = 1;
    }
    for(i=0; i < numConns; i++) {
        if ((new_socket = accept(fd, (struct sockaddr *)&address,
                (socklen_t*)&addrlen))<0) {
            perror("accept");
            return 1;
        }
        int valread = read(new_socket, buffer, 1024);
        bool matched = false;

        if (primServer != NULL) {
            // Looking for one very specific connection
            if(buffer != primServer->getName()) {
                LOG(ERROR) << "Expected Connection from " << primServer->getName() << ", got " << buffer;
                return 1;
            } else {
                LOG(DEBUG2) << "Connection from " << primServer->getName() << " - socket " << new_socket;
                primServer->net_data.socket = new_socket;
                primServer->alive = true;
                matched = true;
            }
        } else {
            // Connection could be from anyone we are backing up, see who it was
            for(auto pserv : primaryServers) {
                if (buffer == pserv->getName()) {
                    LOG(DEBUG2) << "Connection from " << pserv->getName() << " - socket " << new_socket;
                    pserv->net_data.socket = new_socket;
                    matched = true;
                    break;
                }
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
        std::string state_str = {state};
        if(send(new_socket, state_str.c_str(), state_str.size(), 0) < 0) {
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

        if (state == 'p') {
            // We opened this endpoint to recover when a previous primary comes back up as a backup.
            // that former primary has no established connection with us and opened an endpoint, waiting
            // for us to connect to it as our backup. issue connection.
            if(connect_backups(primServer)) return 1;
        }

    }

    return 0;
}

int Server::connect_backups(Server* newBackup /* defaults NULL */ ) {
    // TODO: This will be reworked by network-layer
    if (newBackup == NULL)
        LOG(INFO) << "Connecting to Backups";
    else
        LOG(INFO) << "Connecting to backup " << newBackup->getName();

    int sock;
    struct hostent *he;

    bool updated = false;
    for (auto backup: backupServers) {
        if (newBackup != NULL && newBackup->getName() != backup->getName()) {
            continue;
        }
        if (!backup->alive) {
            LOG(DEBUG2) << "  Skipping down backup " << backup->getName();
            continue;
        }

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

        LOG(DEBUG2) << "    Connection established, sending checksum";

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

        updated = true;

        // Right now, this server expects to be running as primary, and backup as secondary
        // There is a chance that this server previously failed and the backup took over.
        // See if backup is running as primary now. If so, this server is now backing it up instead.
        if (o_state[0] == 'p') {
            // Backup took over at some point. Become a backup to it now.
            // TODO: Verify only one backup server responds with this
            LOG(INFO) << "Backup Server " << backup->getName() << " took over as primary";
            // See if we already are backing this server up on other keys
            bool alreadyBacking = false;
            for (auto p : primaryServers) {
                if (p->getName() == backup->getName()) {
                    // already backing up, add our keys to its keys
                    for (auto kr : primaryKeys)
                        p->addKeyRange(kr);
                    primaryKeys.clear();
                    alreadyBacking = true;
                    break;
                }
            }
            if (!alreadyBacking) {
                // We are a new backup for this server, open new connection
                primaryServers.push_back(backup);
                for (auto kr : primaryKeys)
                    backup->addKeyRange(kr);
                primaryKeys.clear();
                open_backup_endpoints(backup, 'b');
            }
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

    if (newBackup != NULL && !updated) {
        LOG(ERROR) << "Failed connecting to backup " << newBackup->getName();
        return 1;
    }
    LOG(INFO) << "Finished connecting to backups";
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

    cksum = kvcg_config.get_checksum();

    // Mark the key range of backups
    for (auto backup : backupServers) {
        for (auto keyRange : primaryKeys)
            backup->backupKeys.push_back(keyRange);
    }

    printServer(INFO);

    // Open connection for other servers to backup here
    open_backup_eps_thread = std::thread(&Server::open_backup_endpoints, this, nullptr, 'b');

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


   // see what changed after primary/backup negotation
   printServer(DEBUG);

exit:
    if (status)
      shutdownServer(); // shutdown on error
    if (open_backup_eps_thread.joinable())
        open_backup_eps_thread.join();

    return status;
}

void Server::shutdownServer() {
  LOG(INFO) << "Shutting down server";
  shutting_down = true;
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

bool Server::isBackup(int key) {
    for (auto el : backupKeys) {
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

void Server::printServer(LogLevel lvl) {
    // Log this server configuration
    LOG(lvl) << "*************** SERVER CONFIG ***************";
    LOG(lvl) << "Hostname: " << this->getName();
    LOG(lvl) << "Primary Keys:";
    for (auto kr : primaryKeys) {
        LOG(lvl) << "  [" << kr.first << ", " << kr.second << "]";
    }
    LOG(lvl) << "Backup Servers:";
    for (auto backup : backupServers) {
        if (backup->alive)
            LOG(lvl) << "  " << backup->getName();
        else
            LOG(lvl) << "  " << backup->getName()  << " (down)";
        for (auto kr : backup->backupKeys) {
            LOG(lvl) << "    [" << kr.first << ", " << kr.second << "]";
        }
    }
    LOG(lvl) << "Backing up primaries:";
    for (auto primary : primaryServers) {
        if (primary->alive)
          LOG(lvl) << "  " << primary->getName();
        else
          LOG(lvl) << "  " << primary->getName() << " (down)";
        for (auto kr : primary->getPrimaryKeys()) {
            LOG(lvl) << "    [" << kr.first << ", " << kr.second << "]";
        }
        LOG(lvl) << "    Other backups:";
        for (auto ob : primary->getBackupServers())
            LOG(lvl) << "      " << ob->getName();
    }

    LOG(lvl) << "*************** SERVER CONFIG ***************";
}
