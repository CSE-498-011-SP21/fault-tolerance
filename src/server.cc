/*****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/server.h>
#include <faulttolerance/kvcg_config.h>
#include <faulttolerance/fault_tolerance.h>

#include <iostream>
#include <bitset>
#include <algorithm>
#include <assert.h>
#include <chrono>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <sstream>
#include <stdexcept>
#include <string.h>
#include <cstdint>

#include <boost/functional/hash.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/range/combine.hpp>

#include <kvcg_errors.h>
#include <data_t.hh>
#include <RequestTypes.hh>
#include <RequestWrapper.hh>

#include <networklayer/connection.hh>

namespace ft = cse498::faulttolerance;

const auto HOSTNAME = boost::asio::ip::host_name();

std::atomic<bool> shutting_down(false);

// timeout in seconds to assume heartbeat failure
#define HB_TIMEOUT 2

void ft::Server::beat_heart(ft::Server* backup) {
  // Write to our backups memory region that we are alive
  unsigned int count = 0;

  cse498::unique_buf buf(4);
  uint64_t bufKey = 3;
  backup->backup_conn->register_mr(
                    buf,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    bufKey);


  while(!shutting_down) {
    sprintf(buf.get(), "%03d", count); // always send 4 bytes
    LOG(TRACE) << "Sending " << backup->getName() << " heartbeat=" << buf.get() << " (MRKEY:" << backup->heartbeat_key <<", ADDR:" << backup->heartbeat_addr << ")";
    if(!backup->backup_conn->try_write(buf, 4, backup->heartbeat_addr, backup->heartbeat_key)) {
        // Backup must have failed. reissue connect
        LOG(WARNING) << "Backup server " << backup->getName() << " went down";
        backup->alive = false;
        if(std::find(primaryServers.begin(), primaryServers.end(), backup) != primaryServers.end()) {
            LOG(DEBUG3) << "Server " << backup->getName() << " is also a primary, handling in primary_listen";
            return; 
        }
        delete backup->backup_conn;

        // Check original state of failed backup. The failed server could
        // have original been a primary that became our backup after failing.
        // When it fails, it will come back up trying to be primary again.
        // TODO: Run in detached thread in case it never comes back?
        auto elem = std::find(originalPrimaryServers.begin(), originalPrimaryServers.end(), backup);
        if(elem != originalPrimaryServers.end()) {
            // failed server will come back as a primary, open backup endpoint to accept connect_backups()
            LOG(DEBUG3) << backup->getName() << " was originally our primary, open endpoint for it";
            open_backup_endpoints(backup, 'p', 0, nullptr);
        } else {
            // failed server will come back as a backup, issue connect_backups to it
            LOG(DEBUG3) << backup->getName() << " was originally a backup, issue connection to it";
            connect_backups(backup, true);
        }

        return;
    }
    count++;
    if (count > 999) count = 0;
    // don't go crazy spamming the network
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void ft::Server::client_listen() {
  LOG(INFO) << "Waiting for client discovery requests...";
  int offset = 0;
  uint64_t bufKey = 0;
  size_t numRanges;
  while(true) {
    cse498::Connection* conn = new cse498::Connection(
        this->getAddr().c_str(),
        true, this->clientPort, this->provider);
    if(!conn->connect()) {
        LOG(ERROR) << "Client connection failure";
        delete conn;
        continue;
    }

    // Client connected to us, must be discovering leaders
    // Respond with a list of our primary key ranges

    // Ensure primaryKeys are not adjusted by another thread
    primaryKeysLock.lock();

    // calculate buffer size and initialize buffer
    int bufSize = sizeof(size_t) + (primaryKeys.size() * (sizeof(unsigned long long)*2));
    cse498::unique_buf buf(bufSize);
    conn->register_mr(buf, FI_SEND | FI_RECV, bufKey);

    // Load up buffer with key ranges
    // First value will be the number of ranges (size_t)
    // Followed by pairs of unsigned long longs.
    offset = sizeof(size_t);
    numRanges = primaryKeys.size();
    memcpy(buf.get(), (void*)&numRanges, sizeof(size_t));
    for (auto kr : primaryKeys) {
        memcpy(buf.get()+offset, (void*)&(kr.first), sizeof(unsigned long long));
        offset += sizeof(unsigned long long);
        memcpy(buf.get()+offset, (void*)&(kr.second), sizeof(unsigned long long));
        offset += sizeof(unsigned long long);
    }

    // unlock primaryKeys
    primaryKeysLock.unlock();

    // Double-check we allocated the propery amount and primaryKeys were locked
    assert(bufSize == offset);

    // Send buffer to client
    conn->send(buf, offset);

    // Close connection to prepare for next discovery request
    delete conn;
  }
}

void ft::Server::primary_listen(ft::Server* pserver) {
    int r;
    cse498::unique_buf buffer(MAX_LOG_SIZE);
    ft::Server* primServer = pserver; // May change over time
    bool remote_closed = false;
    size_t offset, bytesConsumed;
    char localBuf[pserver->logging_mr.size()];

    auto last_check = std::chrono::steady_clock::now();
    auto curr_time = std::chrono::steady_clock::now();
    int curr_heartbeat = 0;
    int prev_heartbeat = 0;

    while(true) {

        // wait for primary to come to life
        LOG(DEBUG) << "Waiting for initial heart beat from " << primServer->getName();
        while(primServer->heartbeat_mr.get()[0] == '-') {
            //LOG(TRACE) << primServer->getName() << "hb:" << primServer->heartbeat_mr.get();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        LOG(INFO) << "Waiting for backup requests from " << primServer->getName();
        last_check = std::chrono::steady_clock::now();
        remote_closed = false;

        while(true) {
          if (shutting_down) break;

          curr_time = std::chrono::steady_clock::now();

          prev_heartbeat = curr_heartbeat;
          curr_heartbeat = atoi(primServer->heartbeat_mr.get());
          if (curr_heartbeat == prev_heartbeat &&
                  std::chrono::duration_cast<std::chrono::seconds>(curr_time - last_check).count() > HB_TIMEOUT) {
              // heartbeat has not updated within timeout, assume primary died
              LOG(WARNING) << "Heartbeat failure detected for " << primServer->getName();
              remote_closed = true;
              break;
          } else if (curr_heartbeat != prev_heartbeat) {
              // reset heartbeat timeout
              LOG(TRACE) << "Heartbeat:" << primServer->getName() << ": " << prev_heartbeat << "->" << curr_heartbeat;
              last_check = std::chrono::steady_clock::now();
          }

          RequestWrapper<unsigned long long, data_t*>* pkt;

          char msgType = primServer->logging_mr.get()[0];
          uint8_t numLogs = 0;
          if (msgType == 'l') {
            numLogs = primServer->logging_mr.get()[1];
            LOG(DEBUG2) << "Read "<< unsigned(numLogs) << " updates from " << primServer->getName();
            memcpy(localBuf, primServer->logging_mr.get(), primServer->logging_mr.size());
            // prepare for next write immediately
            primServer->logging_mr.get()[0] = '\0';
          } else if (msgType == 'p') {
            // Primary server determined one of its backups took over
            // Start listening to them instead.
            ft::Server* expNewPrimary = nullptr;
            std::string newName = primServer->logging_mr.get()+1;
            LOG(DEBUG2) << "Primary " << primServer->getName() << " determined new primary name is " << newName;
            for (auto &b : primServer->getBackupServers()) {
                if (b->getName() == newName) {
                    expNewPrimary = b;
                    break;
                }
            }
            assert(expNewPrimary != nullptr);
            ft::Server* newPrimary = handlePrimaryFailure(primServer, expNewPrimary);
            if (newPrimary == nullptr) {
                return; // must be listening already in another thread
            }
            primServer = newPrimary;
            continue;
          } else {
            continue;
          }

          offset = 0;
          primServer->logged_putsLock.lock();
          for(int i=0; i < numLogs; i++) {
            pkt = new RequestWrapper<unsigned long long, data_t*>();
            *pkt = deserialize2<RequestWrapper<unsigned long long, data_t*>>(localBuf+2+offset, primServer->logging_mr.size()-2-offset, bytesConsumed);
            offset += bytesConsumed;
            if (pkt->requestInteger == REQUEST_INSERT) {
              LOG(INFO) << "Received from " << primServer->getName() << ": INSERT (" << pkt->key << "," << pkt->value->data << ")";
            } else if (pkt->requestInteger == REQUEST_REMOVE) {
              LOG(INFO) << "Received from " << primServer->getName() << ": REMOVE (" << pkt->key << "," << pkt->value->data << ")";
            } else {
              LOG(ERROR) << "Received unexpected request from " << primServer->getName() << ": " << pkt->requestInteger;
            }


            // Add to queue for this primary server
            auto elem = primServer->logged_puts->find(pkt->key);
            assert(elem != primServer->logged_puts->end());
            LOG(DEBUG4) << "Replacing log entry for " << primServer->getName() << " key " << pkt->key << ": " << elem->second->value->data << "->" << pkt->value->data;
            elem->second->value->size = pkt->value->size;
            elem->second->requestInteger = pkt->requestInteger;
            memcpy(elem->second->value->data, pkt->value->data, pkt->value->size);
            // There isn't a good destructor for this
            delete pkt->value->data;
            delete pkt->value;
            delete pkt;
          }
          primServer->logged_putsLock.unlock();
        }

        if (shutting_down)
            return;

        if (remote_closed) {
            ft::Server* newPrimary = handlePrimaryFailure(primServer);
            if (newPrimary == nullptr || newPrimary->getName() == this->getName()) {
                return;
            } else {
                // continue this thread
                primServer = newPrimary;
            }
        }
    }
}

ft::Server* ft::Server::handlePrimaryFailure(ft::Server* primServer, ft::Server* expNewPrimary /* nullptr */) {
    ft::Server* newPrimary = NULL;
    bool resolved = false;
    int idx = 0;

    while (newPrimary == NULL) {
        // Determine new primary
        newPrimary = NULL;
        for (auto &s : primServer->getBackupServers()) {
            if (expNewPrimary != nullptr && s->getName() == expNewPrimary->getName()) {
              newPrimary = s;
              break;
            } else if (expNewPrimary == nullptr && s->alive) {
              // first alive server takes over
              newPrimary = s;
              break;
            }
            idx++;
        }

        // If we are still running, then there still had to be a backup...
        assert(newPrimary != NULL);

        std::vector<ft::Server*> newPrimaryBackups;

        // rotate vector of backups for who is next in line
       for (int i=idx+1; i < primServer->getBackupServers().size()*2; i++) {
            int backupIdx = i%primServer->getBackupServers().size();
            if (backupIdx == 0) {
                newPrimaryBackups.push_back(primServer);
            }
            if (backupIdx == idx) {
                break;
            }
            newPrimaryBackups.push_back(primServer->getBackupServers()[backupIdx]);
        }

        if (expNewPrimary == nullptr) {
            // Keep old primary on list of backups, but mark it as down
            primServer->alive = false;
        }

        // Log
        LOG(DEBUG) << "New primary: "<< newPrimary->getName();
        LOG(DEBUG2) << "  Backups:";
        for (auto &newBackup : newPrimaryBackups) {
            if(newBackup->alive)
              LOG(DEBUG2) << "    " << newBackup->getName();
            else
              LOG(DEBUG2) << "    " << newBackup->getName() << " (down)";
        }

        if (HOSTNAME == newPrimary->getName()) {
            LOG(INFO) << "Taking over as new primary";
            std::vector<RequestWrapper<unsigned long long, data_t *>> commitBatch;
            assert(this->getName() != primServer->getName());
            this->logged_putsLock.lock();
            primServer->logged_putsLock.lock();
            for (auto it = primServer->logged_puts->begin(); it != primServer->logged_puts->end(); ++it) {
                if (it->second->value->data[0] != '\0') {
                  LOG(DEBUG) << "  Commit: ("  << it->second->key << "," << it->second->value->data << ")";
                  commitBatch.push_back(*(it->second));
                  auto elem = this->logged_puts->find(it->first);
                  //assert(elem->second->value->data[0] == '\0');
                  elem->second->requestInteger = it->second->requestInteger;
                  elem->second->value->size = it->second->value->size;
                  memcpy(elem->second->value->data, it->second->value->data, it->second->value->size);
                  it->second->value->size = 0;
                  it->second->value->data[0] = '\0';
                }
            }
            primServer->logged_putsLock.unlock();
            this->logged_putsLock.unlock();
            if (this->commitFn) {
              LOG(DEBUG) << "Calling commit function of caller";
              this->commitFn(commitBatch);
            }

            // become primary for old primary's keys
            for (auto kr : primServer->primaryKeys) {
                LOG(DEBUG3) << "  Adding key range [" << kr.first << ", " << kr.second << "]";
                addKeyRange(kr);
            }

            // Add new backups to my backups
            for (auto &newBackup : newPrimaryBackups) {
                bool exists = false;
                for (auto &existingBackup : backupServers) {
                    if (existingBackup->getName() == newBackup->getName()) {
                        // this server is already backing us up for our primary keys
                        // add another key range to it
                        LOG(DEBUG2) << "Already backing up to " << newBackup->getName() << ", adding keys";
                        for (auto const &kr : primServer->primaryKeys) {
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
                    for (auto const &kr : primServer->primaryKeys) {
                       LOG(DEBUG3) << "  Adding backup key range [" << kr.first << ", " << kr.second << "]" << " to " << newBackup->getName();
                       newBackup->backupKeys.push_back(kr);
                    }
                    if (newBackup->getName() != primServer->getName()) {
                      // TBD: This is blocking, will wait forever on dead servers
                      if (std::find(originalPrimaryServers.begin(), originalPrimaryServers.end(), newBackup) != originalPrimaryServers.end()) {
                          // This new backup was a failed primary, meaning it will try to connect to us.
                          // open endpoint for it.
                          LOG(DEBUG2) << "  Opening connection to new backup that was the original primary " << newBackup->getName();
                          open_backup_endpoints(newBackup, 'p', 0, nullptr);
                      } else {
                          // Connect to new backup
                          LOG(DEBUG2) << "  Connecting to new backup " << newBackup->getName();
                          connect_backups(newBackup, true); 
                      }
                    }
                }
            }

            primaryServers.erase(std::find(primaryServers.begin(), primaryServers.end(), primServer));

            printServer(DEBUG);

            // Check original state of failed primary. The failed server could
            // have original been a backup that took over as primary. When it fails
            // and comes back up, it will come back up as a backup waiting for us
            // to connect to it. In this case, need to issue connect_backups to it.
            auto elem = std::find(originalPrimaryServers.begin(), originalPrimaryServers.end(), primServer);
            if(elem != originalPrimaryServers.end()) {
                // failed server will come back as a primary, open backup endpoint to accept connect_backups()
                LOG(DEBUG3) << primServer->getName() << " was originally our primary, open endpoint for it"; 
                delete primServer->primary_conn;
                open_backup_endpoints(primServer, 'p', 0, nullptr);
            } else {
                // failed server will come back as a backup, issue connect_backups to it
                LOG(DEBUG3) << primServer->getName() << " was originally a backup, issue connection to it";
                delete primServer->primary_conn;
                connect_backups(primServer, true);
            }

        } else {
            LOG(INFO) << "Not taking over as new primary";
            newPrimary->backupServers = newPrimaryBackups;

            // Copy what we logged for the old primary to what we will log for the new primary
            assert(this->getName() != primServer->getName());
            this->logged_putsLock.lock();
            primServer->logged_putsLock.lock();
            for (auto it = primServer->logged_puts->begin(); it != primServer->logged_puts->end(); ++it) {
                if (it->second->value->data[0] != '\0') {
                  LOG(DEBUG) << "  Copying to " << newPrimary->getName() << ": ("  << it->second->key << "," << it->second->value->data << ")";
                  auto elem = this->logged_puts->find(it->first);
                  elem->second->value->size = it->second->value->size;
                  elem->second->requestInteger = it->second->requestInteger;
                  memcpy(elem->second->value->data, it->second->value->data, it->second->value->size);
                  it->second->value->data[0] = '\0';
                  it->second->value->size = 0;
                }
            }
            primServer->logged_putsLock.unlock();
            this->logged_putsLock.unlock();

            // See if the new primary that took over is new for us, or someone we already back up
            bool exists = false;
            for (auto &existingPrimary : primaryServers) {
                if (existingPrimary->getName() == newPrimary->getName()) {
                    // We are already backing this server up. Update it's primary keys
                    LOG(DEBUG2) << "Already backing up " << newPrimary->getName() << ", adding keys";
                    for (auto const &kr : primServer->primaryKeys) {
                       LOG(DEBUG3) << "  Adding primary key range [" << kr.first << ", " << kr.second << "]" << " to " << newPrimary->getName();
                       existingPrimary->primaryKeys.push_back(kr);
                    }
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                // new primary, will try connecting to us
                LOG(DEBUG2) << newPrimary->getName() << " is a new primary";
                addPrimaryServer(newPrimary);
                for (auto const &kr : primServer->primaryKeys) {
                    LOG(DEBUG3) << "  Adding primary key range [" << kr.first << ", " << kr.second << "]" << " to " << newPrimary->getName();
                    newPrimary->primaryKeys.push_back(kr);
                }
                // Set a timeout
                int ret;
                open_backup_endpoints(newPrimary, 'b', HB_TIMEOUT*3, &ret);
                primaryServers.push_back(newPrimary);
                if (ret == KVCG_ETIMEOUT) {
                  LOG(WARNING) << "Failed getting connection from new primary " << newPrimary->getName();
                  // The backup that was next in line for the old primary may have died before the primary did,
                  // and we did not know it. Treat as if it did take over and now failed, looping back again
                  // to resolve who is next to take over as primary.
                  primServer = newPrimary;
                  newPrimary = NULL;
                } else if (ret) {
                  LOG(ERROR) << "Unknown error getting connection from new primary " << newPrimary->getName() << ": " << ret;
                  return nullptr;
                }
           } else {
              return nullptr; // already listening in another thread
           }
        }
    }

    return newPrimary;

}

int ft::Server::open_backup_endpoints(ft::Server* primServer /* NULL */, char state /*'b'*/, int timeout /* 0 */, int* ret /* NULL */) {
    if (primServer == NULL)
        LOG(INFO) << "Opening backup endpoint for other Primaries";
    else {
        LOG(INFO) << "Opening backup endpoint for " << primServer->getName();
    }
    auto start_time = std::chrono::steady_clock::now();
    int opt = 1;
    int i,j,k;
    int status = KVCG_ESUCCESS;
    int numConns, valread;
    cse498::unique_buf buf;
    uint64_t bufKey = 1;
    cse498::Connection* new_conn;
    bool matched;
    std::string cksum_str, state_str;

    numConns = primaryServers.size();
    if (primServer != NULL) {
        numConns = 1;
    } else {
        // only allow timeout for single node connection
        assert(timeout == 0);
    }
    for(i=0; i < numConns; i++) {
        ft::Server* connectedServer;
        new_conn = new cse498::Connection(
            this->getAddr().c_str(),
            true, this->serverPort, this->provider);

        while(true) {
          auto p = new_conn->nonblockingAccept();
          if (p.first) {
            *new_conn = std::move(p.second);
            break;
          } else if (shutting_down) {
            delete new_conn;
            goto exit;
          } else if (timeout > 0 && std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time).count() > timeout) {
            LOG(ERROR) << "Timed out after " << timeout << " seconds waiting for connection from primary " << primServer->getName();
            status = KVCG_ETIMEOUT;
            delete new_conn;
            goto exit;
          } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
          }
        }

        new_conn->register_mr(buf, FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ, bufKey);

        // receive name
        new_conn->recv(buf, 1024);
        matched = false;

        if (primServer != NULL) {
            // Looking for one very specific connection
            if(buf.get() != primServer->getName()) {
                LOG(ERROR) << "Expected Connection from " << primServer->getName() << ", got " << buf.get();
                *(buf.get()) = 'n';
                new_conn->send(buf, 1);
                // retry
                delete new_conn;
                i = -1;
                continue;
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
                if (buf.get() == pserv->getName()) {
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
                for (j=0; j < primaryServers[i]->getBackupServers().size(); j++) {
                    ft::Server* pbackup = primaryServers[i]->getBackupServers()[j];  
                    if (buf.get() == pbackup->getName()) {
                        LOG(DEBUG2) << "Connection from primary server " << primaryServers[i]->getName() << " backup " << pbackup->getName();
                        matched = true;
                        connectedServer = pbackup;
                        // Store this backup as the new primary for the key range
                        pbackup->primary_conn = new_conn;
                        primaryServers.push_back(pbackup);

                        // The backup that took over for the original primary now has all the
                        // original primary's servers as its backup. Add them in circular order
                        for (k=j+1; (k % primaryServers[i]->getBackupServers().size()) != j; k++) {
                            ft::Server* backupToAdd = primaryServers[i]->getBackupServers()[k % primaryServers[i]->getBackupServers().size()];
                            if ( (k % primaryServers[i]->getBackupServers().size()) == 0) {
                              // insert original primary into backup list here
                              LOG(DEBUG2) << "  Adding " << primaryServers[i]->getName() << " as a backup to " << pbackup->getName();
                              pbackup->addBackupServer(primaryServers[i]);
                            }
                            LOG(DEBUG2) << "  Adding " << backupToAdd->getName() << " as a backup to " << pbackup->getName();
                            pbackup->addBackupServer(backupToAdd);
                        }

                        // Remove the original from servers we are backing up
                        primaryServers.erase(primaryServers.begin()+i);
                        break;
                    }
                }
                if (matched) break;
            }
        }

        if (!matched) {
            LOG(ERROR) << "Received connection from unrecognized server - " << buf.get();
            *(buf.get()) = 'n';
            new_conn->send(buf, 1);
            status = KVCG_EBADCONN;
            goto exit;
        } else {
            *(buf.get()) = 'y';
            new_conn->send(buf, 1);
        }

        // send config checksum
        cksum_str = std::to_string(this->cksum);
        buf.cpyTo(cksum_str.c_str(), cksum_str.size());
        new_conn->send(buf, cksum_str.size());
        // Also send byte to indicate running as a backup
        *(buf.get()) = state;
        new_conn->send(buf, 1);
        // wait for response
        new_conn->recv(buf, cksum_str.size());
        if (std::stoul(cksum_str) == std::stoul(buf.get())) {
            LOG(DEBUG2) << "Config checksum matches";
        } else {
            LOG(ERROR) << " Config checksum from " << connectedServer->getName() << " (" << std::stoul(buf.get()) << ") does not match local (" << std::stoul(cksum_str) << ")";
            status = KVCG_EBADCONFIG;
            goto exit;
        }

        if (state == 'p') {
            // We opened this endpoint to recover when a previous primary comes back up as a backup.
            // that former primary has no established connection with us and opened an endpoint, waiting
            // for us to connect to it as our backup. issue connection.
            if(status = connect_backups(primServer)) {
                goto exit;
            }
        } else {
            // Register memory region for primary to write heartbeat to
            uint64_t heartbeat_key = (uint64_t)boost::hash_value(connectedServer->getName());
            connectedServer->heartbeat_mr.get()[0] = '-';
            LOG(TRACE) << "Registering MRKEY " << heartbeat_key << " for " << connectedServer->getName();
            connectedServer->primary_conn->register_mr(
                    connectedServer->heartbeat_mr,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    heartbeat_key);

            // Send key to primary
            *((uint64_t*)buf.get()) = heartbeat_key;
            new_conn->send(buf, sizeof(heartbeat_key));
            if (this->provider != cse498::Sockets) {
                // Send addr to primary
                *((uint64_t*)buf.get()) = (uint64_t)(connectedServer->heartbeat_mr.get());
                new_conn->send(buf, sizeof(uint64_t));
            }

            // Register memory region for backup logging
            uint64_t logging_mr_key = (uint64_t)boost::hash_value(connectedServer->getName())*2;
            connectedServer->logging_mr.get()[0] = '\0';
            connectedServer->primary_conn->register_mr(
                    connectedServer->logging_mr,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    logging_mr_key);

            // Send key to primary
            *((uint64_t*)buf.get()) = logging_mr_key;
            new_conn->send(buf, sizeof(logging_mr_key));
            if (this->provider != cse498::Sockets) {
                // Send addr to primary
                *((uint64_t*)buf.get()) = (uint64_t)(connectedServer->logging_mr.get());
                new_conn->send(buf, sizeof(uint64_t));
            }
        }

    }

exit:
    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    if (ret != nullptr) *ret = status;
    return status;
}

int ft::Server::logRequest(unsigned long long key, data_t* value) {
    std::vector<unsigned long long> keys {key};
    std::vector<data_t*> values {value};
    return logRequest(keys, values);
}

int ft::Server::logRequest(std::vector<unsigned long long> keys, std::vector<data_t*> values) {
    // Send batch of transactions to backups
    std::vector<RequestWrapper<unsigned long long, data_t *>> batch;

    int idx=-1;
    for (auto tup : boost::combine(keys, values)) {
        idx++;
        data_t* value;
        unsigned long long key;
        boost::tie(key, value) = tup;
        RequestWrapper<unsigned long long, data_t*> pkt{key, 0, value, REQUEST_INSERT};
        batch.push_back(pkt);
    }

    return logRequest(batch);
}

int ft::Server::logRequest(std::vector<RequestWrapper<unsigned long long, data_t *>> batch, std::vector<RequestWrapper<unsigned long long, data_t *>>* failedBatch /* DEFAULT nullptr */) {
    auto start_time = std::chrono::steady_clock::now();
    int status = KVCG_ESUCCESS;
    bool backedUp[batch.size()] = { 0 };
    int logBufSize = 4096;
    int backedUpOffset, idx, offset;
    std::bitset<255> skippedBitmask;
    uint8_t numLogs = 0;

    // TODO: Make parallel
    for (auto backup : backupServers) {
        // TBD: What happens if a backup died during backup process?
        if (!backup->alive) {
            LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
            continue;
        }
        std::unique_lock<std::mutex> lock(backup->logDataBufLock);
        backup->logDataBuf.get()[0] = 'l'; // first byte indicate packet type - 'l'=log
        backup->logDataBuf.get()[1] = '1'; // will indicate number of requests per write

        idx = -1;
        numLogs = 0;
        backedUpOffset = 0;
        skippedBitmask = 0;
        offset = 0;
        for (auto req : batch) {
            idx++;

            if (req.requestInteger != REQUEST_INSERT && req.requestInteger != REQUEST_REMOVE) {
                LOG(DEBUG2) << "Skipping read request (" << req.requestInteger << ")";
                backedUpOffset++;
                goto checklogend;
            }

            if(!backup->isBackup(req.key)) {
                LOG(DEBUG2) << "Skipping backup to server " << backup->getName() << " not tracking key " << req.key;
                skippedBitmask[backedUpOffset] = 1;
                backedUpOffset++;
                goto checklogend;
            }
            if (req.requestInteger == REQUEST_INSERT) {
              LOG(INFO) << "Logging to " << backup->getName() << ":  INSERT (" << req.key << "): " << req.value->data;
            } else {
              LOG(INFO) << "Logging to " << backup->getName() << ":  REMOVE (" << req.key << "): " << req.value->data;
            }

            size_t dataSize;
            try {
                dataSize = serialize2(backup->logDataBuf.get()+2+offset, logBufSize-offset, req);
                if (offset + dataSize > logBufSize) {
                    // serialize2 should've raise an exception, force it
                    throw std::overflow_error("MR buffer filled");
                }
            } catch (const std::overflow_error& e) {
                if (offset == 0) {
                    LOG(ERROR) << "Can not log key " << req.key << ", data too large!";
                    skippedBitmask[backedUpOffset] = 1;
                    backedUpOffset++;
                    status = KVCG_EINVALID;
                    goto checklogend;
                }

                // Filled buffer; send what we have and prepare for next
                LOG(DEBUG3)<< "Filled buffer to " << backup->getName() << ", sending " << (unsigned)numLogs << " logs (" << offset << "+1 bytes)";
                backup->logCheckBufLock.lock();
                do {
                  backup->backup_conn->read(backup->logCheckBuf, 1, backup->logging_mr_addr, backup->logging_mr_key);
                } while (backup->logCheckBuf.get()[0] != '\0');
                backup->logDataBuf.get()[1] = numLogs;
                backup->backup_conn->write(backup->logDataBuf, offset+2, backup->logging_mr_addr, backup->logging_mr_key);
                backup->logCheckBufLock.unlock();
                // mark that these were backed up
                for (int j=(idx-backedUpOffset); j<=idx-1; j++) {
                  LOG(TRACE) << "Setting mark on key[" << j << "]. skippedBitmask=" << skippedBitmask << ", idx=" << idx << ", backedUpOffset=" << backedUpOffset;
                  if (skippedBitmask[j-(idx-backedUpOffset)] == 1) {
                      LOG(DEBUG4) << "Skip marking key[" << j << "] for backup " << backup->getName();
                  } else {
                      LOG(DEBUG4) << "Marking key[" << j << "] for backup " << backup->getName();
                      backedUp[j] = true;
                  }
                }

                // Load up for next key
                offset = 0;
                numLogs = 0;
                backedUpOffset = 0;
                skippedBitmask = 0;
                try {
                  dataSize = serialize2(backup->logDataBuf.get()+2+offset, logBufSize-offset, req);
                  if (offset + dataSize > logBufSize) {
                    // serialize2 should've raise an exception, force it
                    throw std::overflow_error("MR buffer filled");
                  }
                } catch (const std::overflow_error& e) {
                  LOG(ERROR) << "Can not log key " << req.key << ", data too large!";
                  status = KVCG_EINVALID;
                  skippedBitmask[backedUpOffset] = 1;
                  backedUpOffset++;
                  goto checklogend;
                }
            }

            LOG(DEBUG2) << "raw data: " << (void*) (backup->logDataBuf.get()+2+offset);
            LOG(DEBUG2) << "data size: " << dataSize << ", current offset: " << offset;

            offset += dataSize;
            numLogs++;
            backedUpOffset++;

checklogend:
            if (numLogs > 254 || idx == batch.size()-1) {
                LOG(DEBUG3) << "Sending " << (unsigned)numLogs << " logs (" << offset << "+1 bytes) to " << backup->getName();
                // Either at the end of the KV pairs, or max number of logs per send
                // (only 1 byte reserved for numLogs, max 255).
                backup->logCheckBufLock.lock();
                do {
                    backup->backup_conn->read(backup->logCheckBuf, 1, backup->logging_mr_addr, backup->logging_mr_key);
                } while (backup->logCheckBuf.get()[0] != '\0');
                backup->logDataBuf.get()[1] = numLogs;
                backup->backup_conn->write(backup->logDataBuf, offset+2, backup->logging_mr_addr, backup->logging_mr_key);
                backup->logCheckBufLock.unlock();
                // mark that these were backed up
                for (int j=(idx-(backedUpOffset-1)); j<=idx; j++) {
                  LOG(TRACE) << "Setting mark on key[" << j << "]. skippedBitmask=" << skippedBitmask << ", idx=" << idx << ", backedUpOffset=" << backedUpOffset;
                  if (skippedBitmask[j-(idx-(backedUpOffset-1))] == 1) {
                      LOG(DEBUG4) << "Skip marking key[" << j << "] for backup " << backup->getName();
                  } else {
                      LOG(DEBUG4) << "Marking key[" << j << "] for backup " << backup->getName();
                      backedUp[j] = true;
                  }
                }
                // clear out for next key
                offset = 0;
                numLogs = 0;
                backedUpOffset = 0;
                skippedBitmask = 0;
            }
        }
    }

    // set return code and update internal logging record
    // TBD: What if some keys succeeded and others failed? For
    //      now we return an error, but still logged the successful ones.
    this->logged_putsLock.lock();
    for (idx=0; idx < batch.size(); idx++) {
        if (!backedUp[idx]) {
            LOG(ERROR) << "Failed to log key - " << batch.at(idx).key;
            if(failedBatch != nullptr) {
                LOG(DEBUG2) << "Adding failed entry to failedBatch";
                failedBatch->push_back(batch.at(idx));
            }
            if(!status || status == KVCG_EUNAVAILABLE) {
                // If the server tried to log a key that we are not the primary for,
                // return status should be INVALID, so the caller does not retry.
                // isPrimary requires locking, so only call if we have to.
                if (!isPrimary(batch.at(idx).key)) {
                    LOG(DEBUG2) << "Not primary for key - " << batch.at(idx).key;
                    status = KVCG_EINVALID;
                } else {
                    status = KVCG_EUNAVAILABLE;
                }
            }
        } else {
            // track that we logged this so it can be restored if a backup fails
            auto elem = this->logged_puts->find(batch.at(idx).key);
            LOG(DEBUG4) << "Replacing log entry for self key " << batch.at(idx).key << ": " << elem->second->value->data << "->" << batch.at(idx).value->data;
            elem->second->value->size = batch.at(idx).value->size;
            elem->second->requestInteger = batch.at(idx).requestInteger;
            memcpy(elem->second->value->data, batch.at(idx).value->data, elem->second->value->size);
        }
    }
    this->logged_putsLock.unlock();

exit:
    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int ft::Server::connect_backups(ft::Server* newBackup /* defaults NULL */, bool waitForDead /* defaults false */ ) {
    int status = KVCG_ESUCCESS;
    auto start_time = std::chrono::steady_clock::now();
    cse498::unique_buf buf;
    uint64_t bufKey = 1;

    if (newBackup == NULL)
        LOG(INFO) << "Connecting to Backups";
    else
        LOG(INFO) << "Connecting to backup " << newBackup->getName();

    bool updated = false;

    for (auto const &backup: backupServers) {
        if (newBackup != NULL && newBackup->getName() != backup->getName()) {
            continue;
        }
        if (!backup->alive && !waitForDead) {
            LOG(DEBUG2) << "  Skipping down backup " << backup->getName();
            continue;
        }

        // accept byte
        buf.get()[0] = 'n';

        while (buf.get()[0] != 'y') {
          LOG(DEBUG) << "  Connecting to " << backup->getName() << " (addr: " << backup->getAddr() << ")";
          backup->backup_conn = new cse498::Connection(backup->getAddr().c_str(), false, this->serverPort, this->provider);
          while(!backup->backup_conn->connect()) {
              if (shutting_down) {
                  goto exit;
              }
              LOG(TRACE) << "Failed connecting to " << backup->getName() << " - retrying";
              std::this_thread::sleep_for(std::chrono::milliseconds(500));
              delete backup->backup_conn;
              backup->backup_conn = new cse498::Connection(backup->getAddr().c_str(), false, this->serverPort, this->provider);
              //status = KVCG_EBADCONN;
              //goto exit;
          }
          backup->backup_conn->register_mr(buf, FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ, bufKey);

          // send my name to backup
          buf.cpyTo(this->getName().c_str() + '\0', this->getName().size()+1);
          LOG(DEBUG3) << "  Sending name (" << this->getName() << ")" << buf.get() << " to " << backup->getName();
          backup->backup_conn->send(buf, this->getName().size()+1);
          // backup will either accept or reject
          backup->backup_conn->recv(buf, 1);
          if (buf.get()[0] != 'y') {
            LOG(DEBUG) << "  " << backup->getName() << " waiting for someone else. retrying...";
            delete backup->backup_conn;
          }
        }

        backup->alive = true;

        LOG(DEBUG2) << "    Connection established, sending checksum";

        // backup should reply with config checksum and its state
        std::string cksum_str = std::to_string(this->cksum);
        backup->backup_conn->recv(buf, cksum_str.size());
        char* o_cksum = new char[cksum_str.size()];
        buf.cpyFrom(o_cksum, cksum_str.size());
        backup->backup_conn->recv(buf, 1);
        char o_state = buf.get()[0];
        // unconditionally send ours back before checking
        buf.cpyTo(cksum_str.c_str(), cksum_str.size());
        backup->backup_conn->send(buf, cksum_str.size());
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
        if (o_state == 'p') {
            // Backup took over at some point. Become a backup to it now.
            // TODO: Verify only one backup server responds with this
            LOG(INFO) << "Backup Server " << backup->getName() << " took over as primary";

            // Tell any previous backups who we already exchanged with that we were wrong and someone
            // else is primary
            for (auto &b : backupServers) {
                if (b->getName() == backup->getName()) {
                    break;
                }
                LOG(DEBUG) << "Informing " << backup->getName() << " of new primary";
                buf.get()[0] = '0';
                b->backup_conn->write(buf, 1, b->heartbeat_addr, b->heartbeat_key);
                buf.get()[0] = 'p';
                buf.cpyTo(backup->getName().c_str() + '\0', backup->getName().size()+1, 1);
                b->backup_conn->write(buf, 1+backup->getName().size(), b->logging_mr_addr, b->logging_mr_key);
            }

            // See if we already are backing this server up on other keys
            bool alreadyBacking = false;
            for (auto &p : primaryServers) {
                if (p->getName() == backup->getName()) {
                    // already backing up, add our keys to its keys
                    LOG(DEBUG2) << "Already backing up " << backup->getName() << ", adding local keys";
                    primaryKeysLock.lock();
                    for (auto const &kr : primaryKeys) {
                        p->addKeyRange(kr);
                        // Also remove our key range from the list of keys the new primary is backing up
                        p->backupKeys.erase(std::remove(p->backupKeys.begin(), p->backupKeys.end(), kr), p->backupKeys.end());
                    }
                    primaryKeys.clear();
                    primaryKeysLock.unlock();
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
                primaryKeysLock.lock();
                for (auto const &kr : primaryKeys)
                    backup->addKeyRange(kr);
                primaryKeys.clear();
                primaryKeysLock.unlock();
                open_backup_endpoints(backup, 'b', 0, nullptr);
            }

            // All of our backupServers are now backups to the one that took over for us.
            for (auto const &ourBackup : backupServers) {
              if (ourBackup->getName() != backup->getName())
                  backup->backupServers.push_back(ourBackup);
            }
            // insert ourselves at the end of the list
            if (std::find(backup->backupServers.begin(), backup->backupServers.end(), this) == backup->backupServers.end()) {
              backup->backupServers.push_back(this);
            }
            // Not a primary anymore, nobody backing this server up.
            clearBackupServers();
            break;
        } else if (o_state != 'b') {
            LOG(ERROR) << "Could not determine state of " << backup->getName() << " - " << o_state;
            status = KVCG_EBADMSG;
            goto exit;
        } else {
            LOG(DEBUG3) << backup->getName() << " is still running as backup";

            // Receive MR keys from backup
            backup->backup_conn->recv(buf, sizeof(backup->heartbeat_key));
            backup->heartbeat_key = *((uint64_t *)buf.get());
            if (this->provider == cse498::Sockets) {
                backup->heartbeat_addr = 0;
            } else {
                backup->backup_conn->recv(buf, sizeof(uint64_t));
                backup->heartbeat_addr = *((uint64_t *)buf.get());
            }

            backup->backup_conn->recv(buf, sizeof(backup->logging_mr_key));
            backup->logging_mr_key = *((uint64_t *)buf.get());
            if (this->provider == cse498::Sockets) {
                backup->logging_mr_addr = 0;
            } else {
                backup->backup_conn->recv(buf, sizeof(uint64_t));
                backup->logging_mr_addr = *((uint64_t *)buf.get());
            }
        }

    }

    if (newBackup != NULL && !updated) {
        LOG(ERROR) << "Failed connecting to backup " << newBackup->getName();
        status = KVCG_EBADCONN;
        goto exit;
    }

    /* Start updating heartbeat on backups */
    for (auto &backup: backupServers) {
        if (newBackup != NULL && newBackup->getName() != backup->getName()) {
            continue;
        }
        if (!backup->alive) {
            continue;
        }

        LOG(DEBUG) << "Starting heartbeat to " << backup->getName();
        heartbeat_threads.push_back(new std::thread(&ft::Server::beat_heart, this, backup));

        // Set up logging memory regions
        backup->backup_conn->register_mr(
                    backup->logCheckBuf,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    backup->logCheckBufKey);

        backup->backup_conn->register_mr(
                    backup->logDataBuf,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    backup->logDataBufKey);


        // In the case that our backup failed and came back online, or we took
        // over as primary and the old primary is back as a backup to us, we need
        // to send all transactions that have happened to the recovered server.
        LOG(DEBUG) << "Restoring logs to " << backup->getName();
        this->logged_putsLock.lock();
        for (auto it = logged_puts->begin(); it != logged_puts->end(); ++it) {
            if(it->second->value->data[0] != '\0' && backup->isBackup(it->first)) {
                LOG(DEBUG) << "Sending (" << it->second->key << "," << it->second->value->data << ") to " << backup->getName();
                do {
                  backup->backup_conn->read(buf, 1, backup->logging_mr_addr, backup->logging_mr_key);
                } while (buf.get()[0] != '\0');
                buf.get()[0] = 'l';
                buf.get()[1] = 1;
                size_t dataSize = serialize2(buf.get()+2, MAX_LOG_SIZE, *it->second);
                backup->backup_conn->write(buf,
                                           dataSize+2, backup->logging_mr_addr, backup->logging_mr_key);
            }
        }
        this->logged_putsLock.unlock();
    }

    LOG(INFO) << "Finished connecting to backups";


exit:
    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int ft::Server::initialize(std::string cfg_file) {
    int status = KVCG_ESUCCESS;
    int k = 0;
    RequestWrapper<unsigned long long, data_t*>* pkt;
    auto start_time = std::chrono::steady_clock::now();
    bool matched = false;
    std::thread open_backup_eps_thread;

    LOG(INFO) << "Initializing Server";

    KVCGConfig kvcg_config;
    if (status = kvcg_config.parse_json_file(cfg_file))
        goto exit;

    // set original primary and backup lists to never change
    for (auto &s : kvcg_config.getServerList()) {
        s->originalPrimaryServers = s->primaryServers;
        s->originalBackupServers = s->backupServers;
    }

    // Get this server from config
    for (auto &s : kvcg_config.getServerList()) {
        if (s->getName() == HOSTNAME) {
            *this = std::move(*s);
            matched = true;
            break;
        }
    }
    if (!matched) {
        LOG(ERROR) << "Failed to find " << HOSTNAME << " in " << cfg_file;
        status = KVCG_EBADCONFIG;
        goto exit;
    }
    this->provider = kvcg_config.getProvider();
    this->serverPort = kvcg_config.getServerPort();
    this->clientPort = kvcg_config.getClientPort();
    this->cksum = kvcg_config.get_checksum();

    // Mark the key range of backups
    primaryKeysLock.lock();
    for (auto &backup : backupServers) {
        for (auto keyRange : primaryKeys)
            backup->backupKeys.push_back(keyRange);
    }
    primaryKeysLock.unlock();

    // For servers backing us up, purge their list
    // of backups if we are not in it. This way, if we find out they took over
    // as our primary, when they fail, we know it is our turn to take over
    // for our set of keys, not their other backups
    for (auto &backup : backupServers) {
        matched = false;
        for (auto &o_backup : backup->getBackupServers()) {
            if (o_backup->getName() == this->getName()) {
                matched = true;
                break;
            }
        }
        if (!matched) {
            backup->backupServers.clear();
        }
    }
    

    // Reserve memory in log history for our keys and keys of servers we are backing up
    primaryKeysLock.lock();
    for (auto kr : primaryKeys) {
        for(k=kr.first; k <= kr.second; k++) {
            pkt = new RequestWrapper<unsigned long long, data_t*>();
            pkt->key = k;
            pkt->value = new data_t(MAX_LOG_SIZE);
            pkt->value->data[0] = '\0';
            this->logged_puts->insert({k, pkt});
            for (auto &backup : backupServers) {
                pkt = new RequestWrapper<unsigned long long, data_t*>();
                pkt->key = k;
                pkt->value = new data_t(MAX_LOG_SIZE);
                pkt->value->data[0] = '\0';
                backup->logged_puts->insert({k, pkt});
            }
        }
    }
    primaryKeysLock.unlock();
    for (auto &primary : primaryServers) {
        for(auto kr : primary->getPrimaryKeys()) {
          for (k=kr.first; k <= kr.second; k++) {
            pkt = new RequestWrapper<unsigned long long, data_t*>();
            pkt->key = k;
            pkt->value = new data_t(MAX_LOG_SIZE);
            pkt->value->data[0] = '\0';
            this->logged_puts->insert({k, pkt});
            pkt = new RequestWrapper<unsigned long long, data_t*>();
            pkt->key = k;
            pkt->value = new data_t(MAX_LOG_SIZE);
            pkt->value->data[0] = '\0';
            primary->logged_puts->insert({k, pkt});
            for (auto &primBackup : primary->getBackupServers()) {
               if (primBackup->getName() != this->getName()) {
                   primBackup->logged_puts->insert({k, pkt});
               }
            }
          }
        }
    }

    printServer(INFO);

    // Open connection for other servers to backup here
    open_backup_eps_thread = std::thread(&ft::Server::open_backup_endpoints, this, nullptr, 'b', 0, &status);

    // Connect to this servers backups
    if (status = connect_backups())
        goto exit;
    
    open_backup_eps_thread.join();
    if (status)
        goto exit; 

    // Start listening for backup requests
    for (auto &primary : primaryServers) {
        primary_listen_threads.push_back(new std::thread(&ft::Server::primary_listen, this, primary));
    }

    // Start listening for clients
    client_listen_thread = new std::thread(&ft::Server::client_listen, this);


   // see what changed after primary/backup negotation
   printServer(DEBUG);

exit:
    if (status) {
      shutdownServer(); // shutdown on error
    }
    if (open_backup_eps_thread.joinable())
        open_backup_eps_thread.join();

    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

void ft::Server::shutdownServer() {
  LOG(INFO) << "Shutting down server";
  shutting_down = true;
  LOG(DEBUG3) << "Stopping heartbeat";
  for (auto& t : heartbeat_threads) {
    if (t->joinable()) {
	  LOG(DEBUG4) << "Join heartbeat thread: " << t->get_id();
      // FIXME: detach is not really correct, but they will disappear on program termination...
      t->detach();
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

bool ft::Server::addKeyRange(std::pair<unsigned long long, unsigned long long> keyRange) {
  primaryKeysLock.lock();
  primaryKeys.push_back(keyRange);
  primaryKeysLock.unlock();
  return true;
}

bool ft::Server::addPrimaryServer(ft::Server* s) {
  // TODO: Validate input
  primaryServers.push_back(s);
  return true;
}


bool ft::Server::addBackupServer(ft::Server* s) {
  // TODO: Validate input
  backupServers.push_back(s);
  return true;
}


bool ft::Server::isPrimary(unsigned long long key) {
    std::unique_lock<std::mutex> lock(primaryKeysLock);
    for (auto el : primaryKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}

bool ft::Server::isBackup(unsigned long long key) {
    for (auto el : backupKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}

std::size_t ft::Server::getHash() {
    std::size_t seed = 0;

    boost::hash_combine(seed, boost::hash_value(getName()));
    primaryKeysLock.lock();
    for (auto keyRange : primaryKeys) {
        boost::hash_combine(seed, boost::hash_value(keyRange.first));
        boost::hash_combine(seed, boost::hash_value(keyRange.second));
    }
    primaryKeysLock.unlock();
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

void ft::Server::printServer(const LogLevel lvl) {
    // Log this server configuration

    // keep thread safe
    std::stringstream msg;
    msg << "\n";
    msg << "*************** SERVER CONFIG ***************\n";
    msg << "Hostname: " << this->getName()  << "\n";
    msg << "Primary Keys:\n";
    primaryKeysLock.lock();
    for (auto kr : primaryKeys) {
        msg << "  [" << kr.first << ", " << kr.second << "]\n";
    }
    primaryKeysLock.unlock();
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
