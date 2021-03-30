/*****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include <faulttolerance/server.h>
#include <faulttolerance/kvcg_config.h>

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
#include <boost/range/combine.hpp>

#include <kvcg_errors.h>
#include <data_t.hh>
#include <RequestTypes.hh>
#include <RequestWrapper.hh>

#include <networklayer/connection.hh>

const auto HOSTNAME = boost::asio::ip::host_name();

KVCGConfig kvcg_config;
size_t cksum;

std::atomic<bool> shutting_down(false);

void Server::beat_heart(Server* backup) {
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
        delete backup->backup_conn;
        // TODO: run connect_backups in detached thread
        connect_backups(backup, true);
        // connect_backups will start a new beat_heart thread
        return;
    }
    count++;
    if (count > 999) count = 0;
    // don't go crazy spamming the network
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void Server::connHandle(cse498::Connection* conn) {
  LOG(INFO) << "Handling connection";
  cse498::unique_buf buffer;
  uint64_t key = 0;
  conn->register_mr(
        buffer,
        FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
        key);

  conn->recv(buffer, 4096);
  LOG(INFO) << "Read: " << (void*) buffer.get();
}

void Server::client_listen() {
  LOG(INFO) << "Waiting for Client requests...";
  while(true) {
    cse498::Connection* conn = new cse498::Connection(
        this->getAddr().c_str(),
        true, CLIENT_PORT, kvcg_config.getProvider());
    if(!conn->connect()) {
        LOG(ERROR) << "Client connection failure";
        delete conn;
        continue;
    }
    // Wait for initial connection
    cse498::unique_buf buf;
    uint64_t key = 0;
    conn->register_mr(buf, FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ, key);
    conn->recv(buf, 4096);

    // launch handle thread
    std::thread connhandle_thread(&Server::connHandle, this, conn);
    connhandle_thread.detach();
  }
}

void Server::primary_listen(Server* pserver) {
    int r;
    cse498::unique_buf buffer(MAX_LOG_SIZE);
    Server* primServer = pserver; // May change over time
    bool remote_closed = false;


    auto last_check = std::chrono::steady_clock::now();
    auto curr_time = std::chrono::steady_clock::now();
    int curr_heartbeat = 0;
    int prev_heartbeat = 0;
    int timeout = 2;

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

#ifndef FT_ONE_SIDED_LOGGING
        uint64_t key = 1;
        primServer->primary_conn->register_mr(buffer,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    key);
#endif // FT_ONE_SIDED_LOGGING

        while(true) {
          if (shutting_down) break;

          curr_time = std::chrono::steady_clock::now();

          prev_heartbeat = curr_heartbeat;
          curr_heartbeat = atoi(primServer->heartbeat_mr.get());
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

          RequestWrapper<unsigned long long, data_t*>* pkt;

#ifdef FT_ONE_SIDED_LOGGING
          if (primServer->logging_mr.get()[0] != '\0') {
			LOG(DEBUG2) << "Read from " << primServer->getName();
			auto pkt_temp = deserialize<RequestWrapper<unsigned long long, data_t*>>(std::vector<char>(primServer->logging_mr.get(), primServer->logging_mr.get() + primServer->logging_mr.size()));
			pkt = &pkt_temp;
            primServer->logging_mr.get()[0] = '\0';
          } else {
            continue;
          }
#else
          if(primServer->primary_conn->try_recv(buffer, MAX_LOG_SIZE)) {
              LOG(DEBUG2) << "Read from " << primServer->getName() << ": " << buffer.get();
              auto pkt_temp = deserialize<RequestWrapper<unsigned long long, data_t*>>(std::vector<char>(buffer.get(), buffer.get() + buffer.size()));
			  pkt = &pkt_temp;
          } else {
              continue;
          }
#endif // FT_ONE_SIDED_LOGGING

          // reset heartbeat timeout
          last_check = std::chrono::steady_clock::now();
          LOG(INFO) << "Received from " << primServer->getName() << " (" << pkt->key << "," << pkt->value->data << ")";

#if 0 
          /* TESTING PURPOSES ONLY - try_recv is blocking, need way to force failure */
          if (pkt->getKey() == 99) {
           remote_closed = true;
           break;
          }
#endif

          // Add to queue for this primary server
          auto elem = primServer->logged_puts->find(pkt->key);
          if (elem == primServer->logged_puts->end()) {
            LOG(DEBUG4) << "Inserting log entry for " << primServer->getName() << " key " << pkt->key;
            primServer->logged_puts->insert({pkt->key, pkt});
          } else {
            LOG(DEBUG4) << "Replacing log entry for " << primServer->getName() << " key " << pkt->key << ": " << elem->second->value->data << "->" << pkt->value->data;
            elem->second = pkt;
          }
        }

        if (shutting_down)
            return;

        if (remote_closed) {
            Server* newPrimary = NULL;
            for (auto &s : primServer->getBackupServers()) {
                // first alive server takes over
                if (s->alive) {
                    newPrimary = s;
                    break;
                }
            }

            // If we are still running, then there still had to be a backup...
            assert(newPrimary != NULL);

            std::vector<Server*> newPrimaryBackups;

            // rotate vector of backups for who is next in line
            for (int i=1; i< primServer->getBackupServers().size(); i++) {
                newPrimaryBackups.push_back(primServer->getBackupServers()[i]);
            }
            // Keep old primary on list of backups, but mark it as down
            primServer->alive = false;
            newPrimaryBackups.push_back(primServer);

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
                // TODO: Commit log
                for (auto it = primServer->logged_puts->begin(); it != primServer->logged_puts->end(); ++it) {
                    LOG(DEBUG) << "  Commit: ("  << it->second->key << "," << it->second->value->data << ")";
                    // We should not have this in our internal log yet...
                    assert(this->logged_puts->find(it->first) == this->logged_puts->end());
                    this->logged_puts->insert({it->first, it->second});
                }
                primServer->logged_puts->clear();

                // TODO: Verify backups match

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
                          // Connect to new backup. It may be dead, so do not block
                          LOG(DEBUG2) << "  Connecting to new backup " << newBackup->getName();
                          // TBD: don't wait forever on dead backups?
                          connect_backups(newBackup, true); 
                        }
                    }
                }

               // TBD: Remove primServer from primaryServers ?
               //      primServer won't try to back up here, and we won't be listening for it.
               //      Does it matter if it is still listed in primaryServers?

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
                    open_backup_endpoints(primServer, 'p', nullptr);
                } else {
                    // failed server will come back as a backup, issue connect_backups to it
                    LOG(DEBUG3) << primServer->getName() << " was originally a backup, issue connection to it";
                    delete primServer->primary_conn;
                    connect_backups(primServer, true);
                }

                // This thread can exit, not listening anymore from old primary
                return;

            } else {
                LOG(INFO) << "Not taking over as new primary";
                newPrimary->backupServers = newPrimaryBackups;

                // Copy what we logged for the old primary to what we will log for the new primary
                for (auto it = primServer->logged_puts->begin(); it != primServer->logged_puts->end(); ++it) {
                    LOG(DEBUG) << "  Copying to " << newPrimary->getName() << ": ("  << it->second->key << "," << it->second->value << ")";
                    newPrimary->logged_puts->insert({it->second->key, it->second});
                }
                primServer->logged_puts->clear();

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
                    // blocks until new primary connects
                    int ret;
                    open_backup_endpoints(newPrimary, 'b', &ret);
                    if (ret) {
                      LOG(ERROR) << "Failed getting connection from new primary " << newPrimary->getName();
                      return;
                    }
                    primaryServers.push_back(newPrimary);
                }

                if (exists) {
                    return; // already listening in another thread
                } else {
                    // resume this primary_listen thread for the new backup
                    primServer = newPrimary;
                }
            }
        }
    }

}

int Server::open_backup_endpoints(Server* primServer /* NULL */, char state /*'b'*/, int* ret /* NULL */) {
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
    }
    for(i=0; i < numConns; i++) {
        Server* connectedServer;
        new_conn = new cse498::Connection(
            this->getAddr().c_str(),
            true, SERVER_PORT, kvcg_config.getProvider());
        if(!new_conn->connect()) {
            LOG(ERROR) << "Failed establishing connection for backup endpoint";
            status = KVCG_EBADCONN;
            goto exit;
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
                    Server* pbackup = primaryServers[i]->getBackupServers()[j];  
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
                            Server* backupToAdd = primaryServers[i]->getBackupServers()[k % primaryServers[i]->getBackupServers().size()];
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
        cksum_str = std::to_string(cksum);
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
            if (kvcg_config.getProvider() != cse498::Sockets) {
                // Send addr to primary
                *((uint64_t*)buf.get()) = (uint64_t)(connectedServer->heartbeat_mr.get());
                new_conn->send(buf, sizeof(uint64_t));
            }

#ifdef FT_ONE_SIDED_LOGGING
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
            if (kvcg_config.getProvider() != cse498::Sockets) {
                // Send addr to primary
                *((uint64_t*)buf.get()) = (uint64_t)(connectedServer->logging_mr.get());
                new_conn->send(buf, sizeof(uint64_t));
            }
#endif // FT_ONE_SIDED_LOGGING
        }

    }

exit:
    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    if (ret != nullptr) *ret = status;
    return status;
}

int Server::log_put(unsigned long long key, data_t* value) {
    std::vector<unsigned long long> keys {key};
    std::vector<data_t*> values {value};
    return log_put(keys, values);
}

int Server::log_put(std::vector<unsigned long long> keys, std::vector<data_t*> values) {
    // Send batch of transactions to backups
    auto start_time = std::chrono::steady_clock::now();
    int status = KVCG_ESUCCESS;
    std::set<Server*> backedUpToList;
    std::set<unsigned long long> testKeys(keys.begin(), keys.end());

    cse498::unique_buf check(1);
    cse498::unique_buf rawBuf;

    // Validate lengths match of keys/values
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Attempting to log differing number of keys and values";
        status = KVCG_EINVALID;
        goto exit;
    }

    // TBD: Should we support duplicate keys?
    //      Warn for now
    if (testKeys.size() != keys.size()) {
        LOG(WARNING) << "Duplicate keys being logged, assuming vector is ordered";
        //LOG(ERROR) << "Can not log put with duplicate keys!";
        //status = KVCG_EINVALID;
        //goto exit;
    }

    // FIXME: Investigate/fix race condition if multiple clients
    //        issue put at same time.
    // Asynchronously send to all backups, check for success later
    for (auto tup : boost::combine(keys, values)) {
        unsigned long long key;
        data_t* value;
        boost::tie(key, value) = tup;

        LOG(INFO) << "Logging PUT (" << key << "): " << value->data;
        RequestWrapper<unsigned long long,data_t*>* pkt = new RequestWrapper<unsigned long long,data_t*>{key, value, REQUEST_INSERT};

        auto elem = this->logged_puts->find(key);
        if (elem == this->logged_puts->end()) {
          //LOG(DEBUG4) << "Recording having logged key " << key;
          this->logged_puts->insert({key, pkt});
        } else {
          //LOG(DEBUG4) << "Replacing log entry for self key " << pkt->getKey() << ": " << elem->second->getValue() << "->" << pkt->getValue();
          elem->second = pkt;
        }
		
        std::vector<char> serializeData = serialize(*pkt);
		char* rawData = &serializeData[0];
		size_t dataSize = serializeData.size();
        LOG(DEBUG2) << "raw data: " << rawData;
        LOG(DEBUG2) << "data size: " << dataSize;
        rawBuf.cpyTo(rawData, dataSize);
        uint64_t checkKey = 7;
        uint64_t loggingKey = 8;

        if (dataSize > MAX_LOG_SIZE) {
            LOG(ERROR) << "Can not log data size (" << dataSize << ") > " << MAX_LOG_SIZE;
            status = KVCG_EINVALID;
            goto exit;
        }

        for (auto backup : backupServers) {
            if (!backup->isBackup(key)) {
                LOG(DEBUG2) << "Skipping backup to server " << backup->getName() << " not tracking key " << key;
                continue;
            }

            if (backup->alive) {
                LOG(DEBUG) << "Backing up to " << backup->getName();
                // FIXME: Only do once, not for every key
                backup->backup_conn->register_mr(
                    check,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    checkKey);

                backup->backup_conn->register_mr(
                    rawBuf,
                    FI_SEND | FI_RECV | FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    loggingKey);

#ifdef FT_ONE_SIDED_LOGGING
                do {
                  // TODO: Make more parallel, right now do not right to backup mr
                  //       if it has not read previous write
                  backup->backup_conn->read(check, 1, backup->logging_mr_addr, backup->logging_mr_key);
                } while (check.get()[0] != '\0');

                // TBD: Ask network-layer for async write
                backup->backup_conn->write(rawBuf, dataSize, backup->logging_mr_addr, backup->logging_mr_key);
#else
                // FIXME: use async_send... but does not work for some reason
                //backup->backup_conn->async_send(rawBuf, dataSize);
                backup->backup_conn->send(rawBuf, dataSize);
                backedUpToList.insert(backup);
#endif
            } else {
                LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
            }
        }
    }

#ifndef FT_ONE_SIDED_LOGGING
    // Wait for all sends to complete
    for (auto backup : backedUpToList) {
        LOG(DEBUG2) << "Waiting for sends to complete on " << backup->getName();
        backup->backup_conn->wait_for_sends();
    }
#endif

exit:
    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int Server::connect_backups(Server* newBackup /* defaults NULL */, bool waitForDead /* defaults false */ ) {
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
          backup->backup_conn = new cse498::Connection(backup->getAddr().c_str(), false, SERVER_PORT, kvcg_config.getProvider());
          while(!backup->backup_conn->connect()) {
              if (shutting_down) {
                  goto exit;
              }
              LOG(TRACE) << "Failed connecting to " << backup->getName() << " - retrying";
              std::this_thread::sleep_for(std::chrono::milliseconds(500));
              delete backup->backup_conn;
              backup->backup_conn = new cse498::Connection(backup->getAddr().c_str(), false, SERVER_PORT, kvcg_config.getProvider());
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
        std::string cksum_str = std::to_string(cksum);
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
            // See if we already are backing this server up on other keys
            bool alreadyBacking = false;
            for (auto &p : primaryServers) {
                if (p->getName() == backup->getName()) {
                    // already backing up, add our keys to its keys
                    LOG(DEBUG2) << "Alreadying backing up " << backup->getName() << ", adding local keys";
                    for (auto const &kr : primaryKeys) {
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
                for (auto const &kr : primaryKeys)
                    backup->addKeyRange(kr);
                primaryKeys.clear();
                open_backup_endpoints(backup, 'b', nullptr);
            }

            // All of our backupServers are now backups to the one that took over for us.
            for (auto const &ourBackup : backupServers) {
              if (ourBackup->getName() != backup->getName())
                  backup->backupServers.push_back(ourBackup);
            }
            // insert ourselves at the end of the list
            backup->backupServers.push_back(this);
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
            if (kvcg_config.getProvider() == cse498::Sockets) {
                backup->heartbeat_addr = 0;
            } else {
                backup->backup_conn->recv(buf, sizeof(uint64_t));
                backup->heartbeat_addr = *((uint64_t *)buf.get());
            }

#ifdef FT_ONE_SIDED_LOGGING
            backup->backup_conn->recv(buf, sizeof(backup->logging_mr_key));
            backup->logging_mr_key = *((uint64_t *)buf.get());
            if (kvcg_config.getProvider() == cse498::Sockets) {
                backup->logging_mr_addr = 0;
            } else {
                backup->backup_conn->recv(buf, sizeof(uint64_t));
                backup->logging_mr_addr = *((uint64_t *)buf.get());
            }
#endif // FT_ONE_SIDED_LOGGING

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
        heartbeat_threads.push_back(new std::thread(&Server::beat_heart, this, backup));

        // In the case that our backup failed and came back online, or we took
        // over as primary and the old primary is back as a backup to us, we need
        // to send all transactions that have happened to the recovered server.
        if (!logged_puts->empty()) {
            LOG(DEBUG) << "Restoring logs to " << backup->getName();

            for (auto it = logged_puts->begin(); it != logged_puts->end(); ++it) {
                if(backup->isBackup(it->first)) {
                    LOG(DEBUG) << "Sending (" << it->second->key << "," << it->second->value << ") to " << backup->getName();
#ifdef FT_ONE_SIDED_LOGGING
                    do {
                      backup->backup_conn->read(buf, 1, backup->logging_mr_addr, backup->logging_mr_key);
                    } while (buf.get()[0] != '\0');
					std::vector<char> serializeData = serialize(*it->second);
                    buf.cpyTo(&serializeData[0], serializeData.size());
                    backup->backup_conn->write(buf,
                                               serializeData.size(), backup->logging_mr_addr, backup->logging_mr_key);
#else
					std::vector<char> serializeData = serialize(*it->second);
                    buf.cpyTo(&serializeData[0], serializeData.size());
                    backup->backup_conn->send(buf, serializeData.size());
#endif // FT_ONE_SIDED_LOGGING

                }
            }
        }
    }

    LOG(INFO) << "Finished connecting to backups";


exit:
    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

int Server::initialize(std::string cfg_file) {
    int status = KVCG_ESUCCESS;
    auto start_time = std::chrono::steady_clock::now();
    bool matched = false;
    std::thread open_backup_eps_thread;

    LOG(INFO) << "Initializing Server";

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

    cksum = kvcg_config.get_checksum();

    // Mark the key range of backups
    for (auto &backup : backupServers) {
        for (auto keyRange : primaryKeys)
            backup->backupKeys.push_back(keyRange);
    }

    printServer(INFO);

    // Open connection for other servers to backup here
    open_backup_eps_thread = std::thread(&Server::open_backup_endpoints, this, nullptr, 'b', &status);

    // Connect to this servers backups
    if (status = connect_backups())
        goto exit;
    
    open_backup_eps_thread.join();
    if (status)
        goto exit; 

    // Start listening for backup requests
    for (auto &primary : primaryServers) {
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

    int runtime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    LOG(DEBUG) << "time: " << runtime << "us, Exit (" << status << "): " << kvcg_strerror(status);
    return status;
}

void Server::shutdownServer() {
  LOG(INFO) << "Shutting down server";
  shutting_down = true;
  LOG(DEBUG3) << "Stopping heartbeat";
  for (auto& t : heartbeat_threads) {
    if (t->joinable()) {
	  LOG(DEBUG4) << "Join heartbeat thread: " << t->get_id();
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

bool Server::addKeyRange(std::pair<unsigned long long, unsigned long long> keyRange) {
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


bool Server::isPrimary(unsigned long long key) {
    for (auto el : primaryKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}

bool Server::isBackup(unsigned long long key) {
    for (auto el : backupKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}

std::size_t Server::getHash() {
    std::size_t seed = 0;

#ifdef FT_ONE_SIDED_LOGGING
    boost::hash_combine(seed, boost::hash_value(1));
#endif // FT_ONE_SIDED_LOGGING

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
