/*****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/fault_tolerance.h>
#include <iostream>
#include <algorithm>
#include <assert.h>
#include <chrono>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <sstream>
#include <string.h>
#include <boost/functional/hash.hpp>
#include <boost/asio/ip/host_name.hpp>
#include  <faulttolerance/kvcg_config.h>
#include <kvcg_errors.h>
#include <networklayer/connection.hh>

const auto HOSTNAME = boost::asio::ip::host_name();

KVCGConfig kvcg_config;
size_t cksum;

std::atomic<bool> shutting_down(false);

void Server::beat_heart(Server* backup) {
  // Write to our backups memory region that we are alive
  unsigned int count = 0;

  char buf[4];

  while(!shutting_down) {
    sprintf(buf, "%03d", count); // always send 4 bytes
    backup->backup_conn->wait_write(buf, 4, 0, this->heartbeat_key);
    count++;
    if (count > 999) count = 0;
    // don't go crazy spamming the network
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void Server::connHandle(cse498::Connection* conn) {
  LOG(INFO) << "Handling connection";
  char buffer[1024] = {'\0'};
  conn->wait_recv(buffer, 1024);
  LOG(INFO) << "Read: " << (void*) buffer;
}

void Server::client_listen() {
  LOG(INFO) << "Waiting for Client requests...";
  while(true) {
    cse498::Connection* conn = new cse498::Connection();
    // Wait for initial connection
    char *buf = new char[128];
    conn->wait_recv(buf, 128);

    // launch handle thread
    std::thread connhandle_thread(&Server::connHandle, this, conn);
    connhandle_thread.detach();
  }
}

void Server::primary_listen(Server* pserver) {
    int r;
    char buffer[64] = {'\0'};
    Server* primServer = pserver; // May change over time
    bool remote_closed = false;


    auto last_check = std::chrono::steady_clock::now();
    auto curr_time = std::chrono::steady_clock::now();
    int curr_heartbeat = 0;
    int prev_heartbeat = 0;
    int timeout = 2; // seconds before checking server pulse
    char hbbuf[4];

    while(true) {
        LOG(INFO) << "Waiting for backup requests from " << primServer->getName();
        last_check = std::chrono::steady_clock::now();
        remote_closed = false;
        while(true) {
          if (shutting_down) break;

          curr_time = std::chrono::steady_clock::now();

          prev_heartbeat = curr_heartbeat;
          curr_heartbeat = atoi(pserver->heartbeat_mr);
          if (curr_heartbeat == prev_heartbeat &&
                  std::chrono::duration_cast<std::chrono::seconds>(curr_time - last_check).count() > timeout) {
              // heartbeat has not updated within timeout, assume primary died
              LOG(WARNING) << "Heartbeat failure detected for " << primServer->getName();
              remote_closed = true;
              break;
          } else if (curr_heartbeat != prev_heartbeat) {
              // reset heartbeat timeout
              LOG(TRACE) << "Heartbeat:" << primServer->getName() << ": " << prev_heartbeat << "->" << curr_heartbeat;
              last_check = std::chrono::steady_clock::now();
          }

          // FIXME: This is blocking. Discuss with network-layer
          primServer->primary_conn->wait_recv(buffer, 64);

          LOG(DEBUG3) << "Read from " << primServer->getName() << ": " << (void*) buffer;
          // reset hearbeat timeout
          last_check = std::chrono::steady_clock::now();
          // FIXME: Server class probably needs to be templated altogether...
          //        for testing, assume key/values are ints...
          BackupPacket<int, int> pkt(buffer);
          LOG(INFO) << "Received from " << primServer->getName() << " (" << pkt.getKey() << "," << pkt.getValue() << ")";
        }

        if (shutting_down)
            return;

        if (remote_closed) {
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
                for (auto kr : primServer->primaryKeys) {
                    LOG(DEBUG3) << "  Adding key range [" << kr.first << ", " << kr.second << "]";
                    addKeyRange(kr);
                }

                // Add new backups to my backups
                for (auto newBackup : newPrimary->getBackupServers()) {
                    bool exists = false;
                    for (auto existingBackup : backupServers) {
                        if (existingBackup->getName() == newBackup->getName()) {
                            // this server is already backing us up for our primary keys
                            // add another key range to it
                            LOG(DEBUG2) << "Already backing up to " << newBackup->getName() << ", adding keys";
                            for (auto kr : primServer->primaryKeys) {
                                LOG(DEBUG3) << "  Adding backup key range [" << kr.first << ", " << kr.second << "]" << " to " << newBackup->getName();
                                existingBackup->backupKeys.push_back(kr);
                            }
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        // Someone new is backing us up now
                        LOG(DEBUG2) << newBackup->getName() << " is a new backup";
                        addBackupServer(newBackup);
                        for (auto kr : primServer->primaryKeys) {
                            LOG(DEBUG3) << "  Adding backup key range [" << kr.first << ", " << kr.second << "]" << " to " << newBackup->getName();
                            newBackup->backupKeys.push_back(kr);
                        }
                        if (newBackup->alive) {
                          // Connect to new backup
                          LOG(DEBUG2) << "  Connecting to new backup " << newBackup->getName();
                          connect_backups(newBackup);
                        } else {
                          LOG(DEBUG3) << "  Skipping connecting to down backup " << newBackup->getName(); 
                        }
                    }
                }

                printServer(DEBUG);

                // When the old primary comes back up, it will call connect_backups()
                // Be ready to accept it and reply 'p' for state
                delete primServer->primary_conn;
                std::thread t = std::thread(&Server::open_backup_endpoints, this, std::ref(primServer), 'p');
                t.detach(); // it may or may not come back online

                // This thread can exit, not listening anymore from old primary
                return;

            } else {
                LOG(INFO) << "Not taking over as new primary";

                // blocks until new primary connects
                open_backup_endpoints(newPrimary, 'b');
                // copy back vector of primaries
                primaryServers.push_back(newPrimary);

                // resume this primary_listen thread
                primServer = newPrimary;
            }
        }
    }

}

int Server::open_backup_endpoints(Server* primServer /* NULL */, char state /*'b'*/) {
    if (primServer == NULL)
        LOG(INFO) << "Opening backup endpoint for other Primaries";
    else {
        LOG(INFO) << "Opening backup endpoint for " << primServer->getName();
    }
    int opt = 1;
    int i = 0;
    int status = KVCG_ESUCCESS;
    int numConns, valread;
    cse498::Connection* new_conn;
    bool matched;
    char buffer[1024] = {0};
    std::string cksum_str, state_str;

    numConns = primaryServers.size();
    if (primServer != NULL) {
        numConns = 1;
    }
    for(i=0; i < numConns; i++) {
        Server* connectedServer;
        new_conn = new cse498::Connection();

        // Wait for initial connection
        char *buf = new char[128];
        new_conn->wait_recv(buf, 128);

        // receive name
        new_conn->wait_recv(buffer, 1024);
        matched = false;

        if (primServer != NULL) {
            // Looking for one very specific connection
            if(buffer != primServer->getName()) {
                LOG(ERROR) << "Expected Connection from " << primServer->getName() << ", got " << buffer;
                status = KVCG_EUNKNOWN;
                goto exit;
            } else {
                LOG(DEBUG2) << "Connection from " << primServer->getName();
                connectedServer = primServer;
                primServer->primary_conn = new_conn;
                primServer->alive = true;
                matched = true;
            }
        } else {
            // Connection could be from anyone we are backing up, see who it was
            for(auto pserv : primaryServers) {
                if (buffer == pserv->getName()) {
                    LOG(DEBUG2) << "Connection from " << pserv->getName();
                    connectedServer = pserv;
                    pserv->primary_conn = new_conn;
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
                        connectedServer = pbackup;
                        // Store this backup as the new primary for the key range
                        pbackup->primary_conn = new_conn;
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
            status = KVCG_EUNKNOWN;
            goto exit;
        }

        // send config checksum
        cksum_str = std::to_string(cksum);
        new_conn->wait_send(cksum_str.c_str(), cksum_str.size());
        // Also send byte to indicate running as a backup
        state_str = {state};
        new_conn->wait_send(state_str.c_str(), state_str.size());
        // wait for response
        char o_cksum[64];
        new_conn->wait_recv(o_cksum, cksum_str.size());
        if (std::stoul(cksum_str) == std::stoul(o_cksum)) {
            LOG(DEBUG2) << "Config checksum matches";
        } else {
            LOG(ERROR) << " Config checksum from " << buffer << " (" << std::stoul(o_cksum) << ") does not match local (" << std::stoul(cksum_str) << ")";
            status = KVCG_EBADCONFIG;
            goto exit;
        }

        if (state == 'p') {
            // We opened this endpoint to recover when a previous primary comes back up as a backup.
            // that former primary has no established connection with us and opened an endpoint, waiting
            // for us to connect to it as our backup. issue connection.
            if(connect_backups(primServer)) {
                status = KVCG_EUNKNOWN;
                goto exit;
            }
        } else {
            // Register memory region for primary to write heartbeat to
            // TODO: network-layer register_mr only allows one mr per connection.
            //       should have two. One for heartbeat and one for actual backups.
            //       Right now log backup uses 2-sided, so it's not a problem yet.
            connectedServer->heartbeat_mr = new char[4];
            connectedServer->primary_conn->register_mr(connectedServer->heartbeat_mr, 4, FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ, this->heartbeat_key);
        }

    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int Server::connect_backups(Server* newBackup /* defaults NULL */ ) {
    int status = KVCG_ESUCCESS;

    if (newBackup == NULL)
        LOG(INFO) << "Connecting to Backups";
    else
        LOG(INFO) << "Connecting to backup " << newBackup->getName();

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
        std::string hello = "hello\0";
        backup->backup_conn = new cse498::Connection(backup->getName().c_str());
        backup->backup_conn->wait_send(hello.c_str(), hello.length()+1);

        LOG(DEBUG2) << "    Connection established, sending checksum";

        // send my name to backup
        backup->backup_conn->wait_send(this->getName().c_str(), this->getName().size());
        // backup should reply with config checksum and its state
        std::string cksum_str = std::to_string(cksum);
        char o_cksum[64];
        backup->backup_conn->wait_recv(o_cksum, cksum_str.size());
        char o_state[1];
        backup->backup_conn->wait_recv(o_state, 1);
        // unconditionally send ours back before checking
        backup->backup_conn->wait_send(cksum_str.c_str(), cksum_str.size());
        if (std::stoul(cksum_str) == std::stoul(o_cksum)) {
            LOG(DEBUG2) << "Config checksum matches";
        } else {
            LOG(ERROR) << " Config checksum from " << backup->getName() << " (" << std::stoul(o_cksum) << ") does not match local (" << std::stoul(cksum_str) << ")";
            status = KVCG_EBADCONFIG;
            goto exit;
        }

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
                    LOG(DEBUG2) << "Alreadying backing up " << backup->getName() << ", adding local keys";
                    for (auto kr : primaryKeys) {
                        p->addKeyRange(kr);
                        // Also remove our key range from the list of keys the new primary is backing up
                        p->backupKeys.erase(std::remove(p->backupKeys.begin(), p->backupKeys.end(), kr), p->backupKeys.end());
                    }
                    primaryKeys.clear();
                    alreadyBacking = true;
                    break;
                }
            }
            if (!alreadyBacking) {
                // We are a new backup for this server, open new connection
                LOG(DEBUG2) << "Adding " << backup->getName() << " to list of primaries to back up";
                primaryServers.push_back(backup);
                // We should only backup the keys that we previously owned.
                // If this server is also configured as a primary for other keys
                // that we are not backing up, clear them so we do not try to
                // take ownership of them on failover
                backup->primaryKeys.clear();
                for (auto kr : primaryKeys)
                    backup->addKeyRange(kr);
                primaryKeys.clear();
                // Insert ourselves to the back of the list of backups for the new primary
                backup->backupServers.push_back(this);
                open_backup_endpoints(backup, 'b');
            }
            // Not a primary anymore, nobody backing this server up.
            clearBackupServers();
            break;
        } else if (o_state[0] != 'b') {
            LOG(ERROR) << "Could not determine state of " << backup->getName() << " - " << o_state;
            status = KVCG_EUNKNOWN;
            goto exit;
        } else {
            LOG(DEBUG3) << backup->getName() << " is still running as backup";
        }
    }

    if (newBackup != NULL && !updated) {
        LOG(ERROR) << "Failed connecting to backup " << newBackup->getName();
        status = KVCG_EUNKNOWN;
        goto exit;
    }

    /* Start updating heartbeat on backups */
    for (auto backup: backupServers) {
        if (newBackup != NULL && newBackup->getName() != backup->getName()) {
            continue;
        }
        if (!backup->alive) {
            continue;
        }

        LOG(DEBUG) << "Starting heartbeat to " << backup->getName();
        heartbeat_threads.push_back(new std::thread(&Server::beat_heart, this, backup));

    }

    LOG(INFO) << "Finished connecting to backups";


exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int Server::initialize() {
    int status = KVCG_ESUCCESS;
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
        status = KVCG_EBADCONFIG;
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
        primary_listen_threads.push_back(new std::thread(&Server::primary_listen, this, primary));
    }

    // Start listening for clients
    client_listen_thread = new std::thread(&Server::client_listen, this);


   // see what changed after primary/backup negotation
   printServer(DEBUG);

exit:
    if (status) {
      shutdownServer(); // shutdown on error
    }
    if (open_backup_eps_thread.joinable())
        open_backup_eps_thread.join();

    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

void Server::shutdownServer() {
  LOG(INFO) << "Shutting down server";
  shutting_down = true;
  LOG(DEBUG3) << "Stopping heartbeat";
  for (auto& t : heartbeat_threads) {
    if (t->joinable()) {
      t->join();
    }
  }

  if (client_listen_thread != nullptr && client_listen_thread->joinable()) {
    LOG(DEBUG3) << "Closing client thread";
    // FIXME: detach is not really correct, but they will disappear on program termination...
    client_listen_thread->detach();
  }
  LOG(DEBUG3) << "Closing primary listening threads";
  for (auto& t : primary_listen_threads) {
    if (t->joinable()) {
      // FIXME: detach is not really correct, but will disappear on program termination...
      t->detach();
    }
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

void Server::printServer(const LogLevel lvl) {
    // Log this server configuration

    // keep thread safe
    std::stringstream msg;
    msg << "\n";
    msg << "*************** SERVER CONFIG ***************\n";
    msg << "Hostname: " << this->getName()  << "\n";
    msg << "Primary Keys:\n";
    for (auto kr : primaryKeys) {
        msg << "  [" << kr.first << ", " << kr.second << "]\n";
    }
    msg << "Backup Servers:\n";
    for (auto backup : backupServers) {
        if (backup->alive)
            msg << "  " << backup->getName() << "\n";
        else
            msg << "  " << backup->getName()  << " (down)\n";
        for (auto kr : backup->backupKeys) {
            msg << "    [" << kr.first << ", " << kr.second << "]\n";
        }
    }
    msg << "Backing up primaries:\n";
    for (auto primary : primaryServers) {
        if (primary->alive)
          msg << "  " << primary->getName() << "\n";
        else
          msg << "  " << primary->getName() << " (down)\n";
        for (auto kr : primary->getPrimaryKeys()) {
            msg << "    [" << kr.first << ", " << kr.second << "]\n";
        }
        msg << "    Other backups:\n";
        for (auto ob : primary->getBackupServers())
            msg << "      " << ob->getName() << "\n";
    }

    msg << "*************** SERVER CONFIG ***************";
    if (lvl <= INFO)
      LOG(INFO) << msg.str();
    else
      LOG(DEBUG) << msg.str();
    

}
