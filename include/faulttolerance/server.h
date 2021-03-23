#ifndef FAULT_TOLERANCE_SERVER_H
#define FAULT_TOLERANCE_SERVER_H

#include <vector>
#include <set>

#include <kvcg_logging.h>
#include <kvcg_errors.h>
#include <networklayer/connection.hh>

#include <faulttolerance/backup_packet.h>
#include <faulttolerance/node.h>

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

  std::thread *client_listen_thread = nullptr;
  std::vector<std::thread*> primary_listen_threads;
  std::vector<std::thread*> heartbeat_threads;

  char* heartbeat_mr;
  uint64_t heartbeat_key = 114; // random

  void beat_heart(Server* backup);
  void client_listen(); // listen for client connections
  void primary_listen(Server* pserver); // listen for backup request from another primary
  void connHandle(cse498::Connection* conn);
  int open_backup_endpoints(Server* primServer = NULL, char state = 'b', int* ret = NULL);
  int open_client_endpoint();
  int connect_backups(Server* newBackup = NULL);

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
    int status = KVCG_ESUCCESS;
    std::set<Server*> backedUpToList;
    std::set<K> testKeys(keys.begin(), keys.end());

    // Validate lengths match of keys/values
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Attempting to log differing number of keys and values";
        status = KVCG_EUNKNOWN;
        goto exit;
    }

    // TBD: Should we support duplicate keys?
    //      Warn for now
    if (testKeys.size() != keys.size()) {
        LOG(WARNING) << "Duplicate keys being logged, assuming vector is ordered";
        //LOG(ERROR) << "Can not log put with duplicate keys!";
        //status = KVCG_EUNKNOWN;
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
        BackupPacket<K,V> pkt(key, value);
        char* rawData = pkt.serialize();

        size_t dataSize = pkt.getPacketSize();
        LOG(DEBUG4) << "raw data: " << (void*)rawData;
        LOG(DEBUG4) << "data size: " << dataSize;

        for (auto backup : backupServers) {
            if (!backup->isBackup(key)) {
                LOG(DEBUG2) << "Skipping backup to server " << backup->getName() << " not tracking key " << key;
                continue;
            }

            if (backup->alive) {
                LOG(DEBUG) << "Backing up to " << backup->getName();
                backup->backup_conn->async_send(rawData, dataSize);
                backedUpToList.insert(backup);
            } else {
                LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
            }
        }
    }

    // Wait for all sends to complete
    for (auto backup : backedUpToList) {
        LOG(DEBUG2) << "Waiting for sends to complete on " << backup->getName();
        backup->backup_conn->wait_for_sends();
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
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
