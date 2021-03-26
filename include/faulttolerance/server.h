#ifndef FAULT_TOLERANCE_SERVER_H
#define FAULT_TOLERANCE_SERVER_H

#include <vector>
#include <map>
#include <set>
#include <chrono>

#include <kvcg_logging.h>
#include <kvcg_errors.h>
#include <networklayer/connection.hh>

#include <faulttolerance/backup_packet.h>
#include <faulttolerance/node.h>

#define MAX_LOG_SIZE 4096

/**
 *
 * Server Node definition
 *
 */
class Server: public Node {
private:

  // For this instance, tracks primary keys
  std::vector<std::pair<int, int>> primaryKeys;

  // For other servers in backupServers, keys is the ranges they backup for this instance
  std::vector<std::pair<int, int>> backupKeys;

  std::vector<Server*> backupServers; // servers backing up this ones primaries
  std::vector<Server*> primaryServers; // servers whose keys this one is backing up

  // backups and primaries will change over time, but need
  // too track original configuration as well
  std::vector<Server*> originalBackupServers;
  std::vector<Server*> originalPrimaryServers;

  std::thread *client_listen_thread = nullptr;
  std::vector<std::thread*> primary_listen_threads;
  std::vector<std::thread*> heartbeat_threads;

  // backups will add here per primary
  // FIXME: int/int should be K/V
  std::map<int, BackupPacket<int, int>*> *logged_puts = new std::map<int, BackupPacket<int, int>*>();

  cse498::unique_buf heartbeat_mr;
  uint64_t heartbeat_key;

#ifdef FT_ONE_SIDED_LOGGING
  cse498::unique_buf logging_mr;
  uint64_t logging_mr_key;
#endif

  void beat_heart(Server* backup);
  void client_listen(); // listen for client connections
  void primary_listen(Server* pserver); // listen for backup request from another primary
  void connHandle(cse498::Connection* conn);
  int open_backup_endpoints(Server* primServer = NULL, char state = 'b', int* ret = NULL);
  int open_client_endpoint();
  int connect_backups(Server* newBackup = NULL, bool waitForDead = false);

  /**
   *
   * Clear list of backup servers
   *
   */
  void clearBackupServers() {
    // TBD: Use smart pointers instead
    // All servers still referenced in kvcg_config.serverList,
    // do not free here
    backupServers.clear();
  }

public:
  cse498::Connection* primary_conn;
  cse498::Connection* backup_conn;

  ~Server() { shutdownServer(); }
  Server() = default;

  // Need custom move constructor
  Server& operator=(const Server&& src) {
    hostname = std::move(src.hostname);
    primaryKeys = std::move(src.primaryKeys);
    backupKeys = std::move(src.backupKeys);
    backupServers = std::move(src.backupServers);
    primaryServers = std::move(src.primaryServers);
    originalBackupServers = std::move(src.originalBackupServers);
    originalPrimaryServers = std::move(src.originalPrimaryServers);
    client_listen_thread = std::move(src.client_listen_thread);
    primary_listen_threads = std::move(src.primary_listen_threads);
    heartbeat_threads = std::move(src.heartbeat_threads);
    //heartbeat_mr = std::move(src.heartbeat_mr);
    heartbeat_key = std::move(src.heartbeat_key);
#ifdef FT_ONE_SIDED_LOGGING
    //logging_mr = std::move(src.logging_mr);
    logging_mr_key = std::move(src.logging_mr_key);
#endif // FT_ONE_SIDED_LOGGING
    logged_puts = std::move(src.logged_puts);

    return *this;
  }

  /**
   *
   * Initialize server
   *
   * @return status. 0 on success, non-zero otherwise.
   *
   */
  int initialize();

  /**
   *
   * Shutdown server
   *
   */
  void shutdownServer();

  /**
   *
   * Print server configuration if log level > lvl
   *
   * @param lvl - log level to start printing. Will print more at higher levels.
   *
   */
  void printServer(const LogLevel lvl);

  /**
   *
   * Get vector of primary key ranges
   *
   * @return vector of min/max key range pairs
   *
   */
  std::vector<std::pair<int, int>> getPrimaryKeys() { return primaryKeys; }

  /**
   *
   * Add key range to primary list
   *
   * @param keyRange - pair of min and max key
   *
   * @return true if added successfully, false otherwise
   *
   */
  bool addKeyRange(std::pair<int, int> keyRange);

  /**
   *
   * Add server who this one is backing up
   *
   * @param s - Server to add to primary server list
   *
   * @return true if added successfully, false otherwise
   *
   */
  bool addPrimaryServer(Server* s);

  /**
   *
   * Get list of servers acting as this one's backup
   *
   * @return vector of backup servers
   *
   */
  std::vector<Server*> getBackupServers() { return backupServers; }

  /**
   *
   * Add server who is backing this one up
   *
   * @param s - Server to add to backup server list
   *
   * @return true if added successfully, false otherwise
   *
   */
  bool addBackupServer(Server* s);

  /**
   * 
   * Check if server is running as primary
   * for a given key
   *
   * @param key - key to check if primary
   * 
   * @return true if primary, false otherwise
   *
   */
  bool isPrimary(int key);

  /**
   *
   * Check if server is backing up a given key
   *
   * @param key - key to check if backing up
   *
   * @return true if backing, false otherwise
   *
   */
  bool isBackup(int key);

  /**
   *
   * Log a PUT transaction to all backup servers.
   *
   * @param key - value of key in table
   * @param value - data to store in table at key
   *
   * @return 0 on success, non-zero on failure
   *
   */
  template <typename K, typename V>
  int log_put(K key, V value) {
    std::vector<K> keys {key};
    std::vector<V> values {value};
    return log_put(keys, values);
  }

  /**
   *
   * Log a batch of PUT transactions to backup servers.
   *
   * @param keys - vector of keys to update
   * @param values - vector of data to store at keys
   *
   * @return 0 on success, non-zero on failure
   *
   */
  template <typename K, typename V>
  int log_put(std::vector<K> keys, std::vector<V> values) {
    // Send batch of transactions to backups
    auto start_time = std::chrono::steady_clock::now();
    int status = KVCG_ESUCCESS;
    std::set<Server*> backedUpToList;
    std::set<K> testKeys(keys.begin(), keys.end());

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
        K key;
        V value;
        boost::tie(key, value) = tup;

        LOG(INFO) << "Logging PUT (" << key << "): " << value;
        BackupPacket<K,V>* pkt = new BackupPacket<K,V>(key, value);

        auto elem = this->logged_puts->find(key);
        if (elem == this->logged_puts->end()) {
          //LOG(DEBUG4) << "Recording having logged key " << key;
          this->logged_puts->insert({key, pkt});
        } else {
          //LOG(DEBUG4) << "Replacing log entry for self key " << pkt->getKey() << ": " << elem->second->getValue() << "->" << pkt->getValue();
          elem->second = pkt;
        }

        char* rawData = pkt->serialize();
        size_t dataSize = pkt->getPacketSize();
        LOG(DEBUG4) << "raw data: " << (void*)rawData;
        LOG(DEBUG4) << "data size: " << dataSize;
        rawBuf.cpyTo(rawData, dataSize);
        uint64_t checkKey = 7;

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
                    FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
                    checkKey);

                backup->backup_conn->register_mr(
                    rawBuf,
                    FI_WRITE | FI_REMOTE_WRITE | FI_READ | FI_REMOTE_READ,
#ifdef FT_ONE_SIDED_LOGGING
                    this->logging_mr_key);
#else
                    (uint64_t) 8);
#endif // FT_ONE_SIDED_LOGGING


#ifdef FT_ONE_SIDED_LOGGING
                do {
                  // TODO: Make more parallel, right now do not right to backup mr
                  //       if it has not read previous write
                  backup->backup_conn->read(check, 1, 0, this->logging_mr_key);
                } while (check.get()[0] != '\0');

                // TBD: Ask network-layer for async write
                backup->backup_conn->write(rawBuf, dataSize, 0, this->logging_mr_key);
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

  /**
   *
   * Get a hash value of this server configuration
   *
   * @return hash of the server
   *
   */
  std::size_t getHash();

};

#endif //FAULT_TOLERANCE_SERVER_H
