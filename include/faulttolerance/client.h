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
  cse498::ProviderType provider;

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

    cse498::unique_buf rawBuf(dataSize);
    rawBuf.cpyTo(rawData, dataSize);
    server->primary_conn->send(rawBuf, dataSize);

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
   * Get the shard storing the key
   *
   * @param key - key whose shard to search for
   *
   * @return shard storing key
   *
   */
  Shard* getShard(unsigned long long key);

  /**
   *
   * Get the new primary server storing a key by broadcasting to the servers in a shard
   *
   * @param shard - shard whose servers to broadcast to
   *
   * @return new primary server storing the key
   *
   */
  Server* getPrimaryOnFailure(unsigned long long key, Shard* shard);

#endif //FAULT_TOLERANCE_CLIENT_H
