/****************************************************
 *
 * Fault Tolerance API Definitions
 *
 ****************************************************/

#ifndef FAULT_TOLERANCE_H
#define FAULT_TOLERANCE_H

/**
 * @file
 *
 * @brief Public API for KVCG Fault Tolerance protocol
 *
 */
#include <cstring>
#include <thread>
#include <string>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <boost/range/combine.hpp>
#include <unistd.h>
#include "kvcg_logging.h"

extern std::string CFG_FILE;

// TODO: Work with Networking-Layer on this
// TODO: Handle multiple keys
/**
 *
 * Packet definition for sending transaction log
 * from primary server to backup servers
 *
 */
template <typename K, typename V>
class BackupPacket {
private:
  char* serialData;

public:
  /**
   *
   * Create a packet for decoded data (sender side)
   *
   * @param key - key in table to update
   * @param value - data to store in table
   *
   */
  BackupPacket(K key, V value) { // sender side
    serialData = NULL;
    this->key = key;
    this->value = value;
  }

  /**
   *
   * Create a pacekt from raw data (receiver side)
   *
   * @param rawData - bytes received to be decoded
   *
   */
  BackupPacket(char* rawData) { // receiver side
      //serialData = rawData;
      memcpy(&this->key, rawData, sizeof(K));
      memcpy(&this->value, rawData+sizeof(K), sizeof(V));
  }

  /**
   *
   * Destructor, free serial data if malloc'd
   *
   */
  ~BackupPacket() {
    if (serialData != NULL)
      free(serialData);
  }

  /**
   *
   * Serialize packet into raw bytes
   *
   * @return raw byte string to send on wire
   *
   */
  char* serialize() {
    if (serialData == NULL) {
      serialData = (char*) malloc(sizeof(key) + sizeof(value));
      memcpy(serialData, (char*)&key, sizeof(key));
      memcpy(serialData+sizeof(key), (char*)&value, sizeof(value));
    }
    return serialData;
  }

  /**
   *
   * Get key value of packet
   *
   * @return key
   *
   */
  K getKey() { return key; }

  /**
   *
   * Get data value of packet
   *
   * @return value
   *
   */
  V getValue() { return value; }

  /**
   *
   * Get size of packet
   *
   * @return packet size
   *
   */
  size_t getPacketSize() {
    size_t pktSize = sizeof(key) + sizeof(value);
    return pktSize;
  }

private:
  K key;
  V value;
};

/**
 *
 * Base class for Server and Client
 *
 */
class Node {
protected:
  std::string hostname;
public:

  // Indicator if node is alive or not
  bool alive = true;

  /**
   *
   * Initialize node data
   *
   */
  virtual int initialize() { return 0; }

  /**
   *
   * Set the name of the node
   *
   * @param n - Name to set for node
   *
   */
  void setName(std::string n) { hostname = n; }

  /**
   *
   * Get the name of the node
   *
   * @return Name of the node
   *
   */
  std::string getName() { return hostname; }

  bool operator < (const Node& o) const { return hostname < o.hostname; }
};

// FIXME: This is placeholder for network-layer 
struct net_data_t {
  int server_fd;
  struct sockaddr_in address;
  int socket; // used for Server instances of backups
};

/**
 *
 * Server Node definition
 *
 */
class Server: public Node {
private:
  std::vector<std::pair<int, int>> primaryKeys; // primary key ranges

  std::vector<Server*> backupServers; // servers backing up this ones primaries
  std::vector<Server*> primaryServers; // servers whose keys this one is backing up

  net_data_t net_data;

  std::thread *client_listen_thread = nullptr;
  std::vector<std::thread*> primary_listen_threads;

  void client_listen(); // listen for client connections
  void primary_listen(Server* pserver); // listen for backup request from another primary
  void connHandle(int socket);
  int open_backup_endpoints();
  int open_client_endpoint();
  int connect_backups();

  /**
   *
   * Clear list of backup servers
   *
   */
  void clearBackupServers() { backupServers.clear(); }

public:
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
   * @return true if primary, false otherwise
   *
   */
  bool isPrimary(int key);

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
        if (backup->alive) {
           LOG(DEBUG) << "Backing up to " << backup->getName();
            send(backup->net_data.socket, rawData, dataSize, 0);
        } else {
            LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
        }
    }

    return true;
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
            if (backup->alive) {
                LOG(DEBUG) << "Backing up to " << backup->getName();
                send(backup->net_data.socket, rawData, dataSize, 0);
            } else {
                LOG(DEBUG2) << "Skipping backup to down server " << backup->getName();
            }
        }
    }

    return true;
  }

  /**
   *
   * Get a hash value of this server configuration
   *
   * @return hash of the server
   *
   */
  std::size_t getHash();

  /**
   *
   * Get net data for this server
   *
   * @return net_data_t struct
   *
   */
  net_data_t getNetData() {
    return this->net_data;
  }

  /**
   *
   * Set the socket fd for this servers net data
   *
   * @param socket - socket fd to store
   *
   */
  void setSocket(int socket) {
    this->net_data.socket = socket;
  }
};

/**
 *
 * Client Node definition
 *
 */
class Client: public Node {
private:
  std::vector<Server*> serverList;

public:
  /**
   *
   * Initialize client
   *
   * @return status. 0 on success, non-zero otherwise.
   *
   */
  int initialize();

  /**
   *
   * Connect to servers
   *
   * @return status. 0 on success, non-zero otherwise.
   *
   */
  int connect_servers();

  /**
   *
   * Store key/value pair in hash table on servers
   *
   * @param key - key to store value at in table
   * @param value - data value to store in table at key
   *
   * @return status. 0 on success, non-zero otherwise.
   *
   */
  template <typename K, typename V>
  int put(K key, V value) {
    // Send transaction to server
    LOG(INFO) << "Sending PUT (" << key << "): " << value;
    BackupPacket<K,V> pkt(key, value);
    char* rawData = pkt.serialize();

    size_t dataSize = pkt.getPacketSize();
    LOG(DEBUG4) << "raw data: " << (void*)rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    Server* server = this->getPrimary(key);

    int status;

    if (send(server->getNetData().socket, rawData, dataSize, 0) < 0) {
      // Send failed
      status = 1;
      return status;
    }

    return 0;
  }

  /**
   *
   * Get value in hash table on servers at key
   *
   * @param key - key to lookup in table
   *
   * @return value stored in table
   *
   */
  template <typename K, typename V>
  V get(K key) {
    // 1. Generate packet to be sent

    // 2. Determine which server is primary

    // 3. Send packet to primary server

    // 4. If failed goto 2. Maybe only try a fixed number of times

    // 5. Return value

    V value;

    return value;
  }

  /**
   *
   * Get the primary server storing a key
   *
   * @param key - key whose primary server to search for
   *
   * @return Server storing key
   *
   */
  template <typename K>
  Server* getPrimary(K key) {
    for (auto server : serverList) {
      if (!server->alive)
        continue;

      if (server->isPrimary(key)) {
        bool server_is_up;

        if (server_is_up) {
          return server;
        }
        else {
          for (auto backup : server->getBackupServers()) {
            // TBD: Send request to promote to shard leader?
            // Right now servers detect socket closure from
            // their primary
          }
        }
      }
    }
    return nullptr;
  }
};

#endif // FAULT_TOLERANCE_H
