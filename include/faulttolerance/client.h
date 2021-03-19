#ifndef FAULT_TOLERANCE_CLIENT_H
#define FAULT_TOLERANCE_CLIENT_H

#include <vector>

#include <kvcg_logging.h>
#include <kvcg_errors.h>
#include <networklayer/connection.hh>

#include <faulttolerance/node.h>
#include <faulttolerance/server.h>
#include <faulttolerance/shard.h>

/**
 *
 * Client Node definition
 *
 */
class Client: public Node {
private:
  std::vector<Shard*> shardList;
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

    server->primary_conn->wait_send(rawData, dataSize);

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
    // TODO: add a primary server vector to kvcg_config.h --> primaryServerList; OR in initialization,
    //    iterate thru serverList & determine which are primary servers (w isPrimary(key) method)
    for (auto server : primaryServerList) {
      if (server->isPrimary(key)) {
        if (server->alive) {
          if ( /* TODO: connect to primary to verify it is alive and the primary */ ) {
            return server;
          } else {
            // copy the code from the "else" below (broadcast to backups, find new primary, add/remove from primaryServerList vector)
            // even need to have it below?
          } 
        }
        else {
          for (auto backup : server->getBackupServers()) {
            // TODO: broadcast to all the servers in the shard- one server (the primary) should respond
            //    note- not sure if this will end up being in a loop
            //    using the broadcast funcs in connectionless.hh (network layer repo) -- how?

            Server* newPrimary = NULL; // or the name of the server, to match to a server in serverList
          }
          int count = 0;
          for(auto s : primaryServerList) {
            if (s->getName() == server->getName()) {
              primaryServerList.erase(primaryServerList.begin() + count);
              break;
            }
            count++;
          }
          primaryServerList.push_back(newPrimary);
          return primaryServerList;
        }
      }
    }
    return nullptr;
  }
};

#endif //FAULT_TOLERANCE_CLIENT_H
