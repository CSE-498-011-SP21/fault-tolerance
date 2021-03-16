#ifndef FAULT_TOLERANCE_CLIENT_H
#define FAULT_TOLERANCE_CLIENT_H

#include <vector>

#include <kvcg_logging.h>
#include <kvcg_errors.h>

#include <faulttolerance/ft_networking.h>
#include <faulttolerance/node.h>
#include <faulttolerance/server.h>

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
    int status = KVCG_ESUCCESS;

    LOG(INFO) << "Sending PUT (" << key << "): " << value;
    BackupPacket<K,V> pkt(key, value);
    char* rawData = pkt.serialize();

    size_t dataSize = pkt.getPacketSize();
    LOG(DEBUG4) << "raw data: " << (void*)rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    Server* server = this->getPrimary(key);

    if (kvcg_send(server->net_data.conn, rawData, dataSize, 0) < 0) {
      // Send failed
      LOG(ERROR) << "Failed sending PUT to " << server->getName();
      status = KVCG_EUNKNOWN;
      goto exit;
    }

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;
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
          }
        }
      }
    }
    return nullptr;
  }
};

#endif //FAULT_TOLERANCE_CLIENT_H
