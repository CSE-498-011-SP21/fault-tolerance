#ifndef FAULT_TOLERANCE_SERVER_H
#define FAULT_TOLERANCE_SERVER_H

#include <vector>
#include <thread>

#include <boost/range/combine.hpp>

#include <kvcg_logging.h>
#include <kvcg_errors.h>

#include <faulttolerance/backup_packet.h>
#include <faulttolerance/node.h>
#include <faulttolerance/ft_networking.h>

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

  void client_listen(); // listen for client connections
  void primary_listen(Server* pserver); // listen for backup request from another primary
  void connHandle(kvcg_addr_t addr);
  int open_backup_endpoints(Server* primServer = NULL, char state = 'b');
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

  // Hold all networking information for server
  net_data_t net_data;

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

    int status = KVCG_ESUCCESS;

    // Send transaction to backups
    LOG(INFO) << "Logging PUT (" << key << "): " << value;
    BackupPacket<K,V> pkt(key, value);
    char* rawData = pkt.serialize();

    size_t dataSize = pkt.getPacketSize();
    LOG(DEBUG4) << "raw data: " << (void*)rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    // TODO: Backup in parallel - dependent on network-layer
    // datagram support
    for (auto backup : backupServers) {
        // check if backing up this key
        if (!backup->isBackup(key)) {
            LOG(DEBUG2) << "Skipping backup to server " << backup->getName() << " not tracking key " << key;
            continue;
        }

        if (backup->alive) {
            LOG(DEBUG) << "Backing up to " << backup->getName();
            if(kvcg_send(backup->net_data.addr, rawData, dataSize, 0) < 0) {
                LOG(ERROR) << "Failed backing up to " << backup->getName();
                status = KVCG_EUNKNOWN;
                goto exit;
            }
        } else {
            LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
        }
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
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

    //TODO: Validate lengths match of keys/values

    // TODO: Parallelize
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

        // TODO: Backup in parallel - dependent on network-layer
        // datagram support
        for (auto backup : backupServers) {
            if (!backup->isBackup(key)) {
                LOG(DEBUG2) << "Skipping backup to server " << backup->getName() << " not tracking key " << key;
                continue;
            }

            if (backup->alive) {
                LOG(DEBUG) << "Backing up to " << backup->getName();
                if(kvcg_send(backup->net_data.addr, rawData, dataSize, 0) < 0) {
                    LOG(ERROR) << "Failed backing up to " << backup->getName();
                    status = KVCG_EUNKNOWN;
                    goto exit;
                }
            } else {
                LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
            }
        }
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