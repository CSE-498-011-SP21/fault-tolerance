#ifndef FAULT_TOLERANCE_SERVER_H
#define FAULT_TOLERANCE_SERVER_H

#include <vector>
#include <thread>
#include <map>
#include <set>
#include <chrono>

#include <kvcg_logging.h>
#include <kvcg_errors.h>

#include <networklayer/connection.hh>

#include <data_t.hh>
#include <RequestTypes.hh>
#include <RequestWrapper.hh>

#include <faulttolerance/node.h>

#define MAX_LOG_SIZE 4096

// Forward declare Server in namespace
namespace cse498 {
  namespace faulttolerance {
    class Server;
  }
}

namespace ft = cse498::faulttolerance;

/**
 *
 * Server Node definition
 *
 */

class ft::Server: public ft::Node {
private:
  // For server-server communication
  int serverPort;

  // For this instance, tracks primary keys
  std::vector<std::pair<unsigned long long, unsigned long long>> primaryKeys;

  // For other servers in backupServers, keys is the ranges they backup for this instance
  std::vector<std::pair<unsigned long long, unsigned long long>> backupKeys;

  std::vector<ft::Server*> backupServers; // servers backing up this ones primaries
  std::vector<ft::Server*> primaryServers; // servers whose keys this one is backing up

  // backups and primaries will change over time, but need
  // too track original configuration as well
  std::vector<ft::Server*> originalBackupServers;
  std::vector<ft::Server*> originalPrimaryServers;

  std::thread *client_listen_thread = nullptr;
  std::vector<std::thread*> primary_listen_threads;
  std::vector<std::thread*> heartbeat_threads;

  // backups will add here per primary
  // FIXME: int/int should be K/V
  std::map<unsigned long long, RequestWrapper<unsigned long long, data_t*>*> *logged_puts = new std::map<unsigned long long, RequestWrapper<unsigned long long, data_t*>*>();

  cse498::unique_buf heartbeat_mr;
  uint64_t heartbeat_key;
  uint64_t heartbeat_addr;

  // when logging,
  cse498::unique_buf logCheckBuf;  // read remote check byte here
  cse498::unique_buf logDataBuf;   // copy log data to send to remote here
  // random unused keys
  uint64_t logCheckBufKey = 44;
  uint64_t logDataBufKey = 55;

  cse498::unique_buf logging_mr;
  uint64_t logging_mr_key;
  uint64_t logging_mr_addr;

  void beat_heart(ft::Server* backup);
  void client_listen(); // listen for client connections
  void primary_listen(ft::Server* pserver); // listen for backup request from another primary
  int open_backup_endpoints(ft::Server* primServer = NULL, char state = 'b', int* ret = NULL);
  int open_client_endpoint();
  int connect_backups(ft::Server* newBackup = NULL, bool waitForDead = false);

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
  Server& operator=(const ft::Server&& src) {
    hostname = std::move(src.hostname);
    addr = std::move(src.addr);
    cksum = std::move(src.cksum);
    provider = std::move(src.provider);
    clientPort = std::move(src.clientPort);
    serverPort = std::move(src.serverPort);
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
    heartbeat_addr = std::move(src.heartbeat_addr);
    //logging_mr = std::move(src.logging_mr);
    logging_mr_key = std::move(src.logging_mr_key);
    logging_mr_addr = std::move(src.logging_mr_addr);
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
  int initialize(std::string cfg_file);

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
  std::vector<std::pair<unsigned long long, unsigned long long>> getPrimaryKeys() { return primaryKeys; }

  /**
   *
   * Add key range to primary list
   *
   * @param keyRange - pair of min and max key
   *
   * @return true if added successfully, false otherwise
   *
   */
  bool addKeyRange(std::pair<unsigned long long, unsigned long long> keyRange);

  /**
   *
   * Add server who this one is backing up
   *
   * @param s - Server to add to primary server list
   *
   * @return true if added successfully, false otherwise
   *
   */
  bool addPrimaryServer(ft::Server* s);

  /**
   *
   * Get list of servers acting as this one's backup
   *
   * @return vector of backup servers
   *
   */
  std::vector<ft::Server*> getBackupServers() { return backupServers; }

  /**
   *
   * Add server who is backing this one up
   *
   * @param s - Server to add to backup server list
   *
   * @return true if added successfully, false otherwise
   *
   */
  bool addBackupServer(ft::Server* s);

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
  bool isPrimary(unsigned long long key);

  /**
   *
   * Check if server is backing up a given key
   *
   * @param key - key to check if backing up
   *
   * @return true if backing, false otherwise
   *
   */
  bool isBackup(unsigned long long key);

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
  int logRequest(unsigned long long key, data_t* value);

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
  int logRequest(std::vector<unsigned long long> keys, std::vector<data_t*> values);

  /**
   *
   * Log a batch of RequestWrapper transactions to backup servers.
   *
   * @param batch - batch of backup requests
   *
   * @return 0 on success, non-zero on failure
   *
   */
  int logRequest(std::vector<RequestWrapper<unsigned long long, data_t *>> batch);

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
