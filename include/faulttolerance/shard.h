#ifndef FAULT_TOLERANCE_SHARD_H
#define FAULT_TOLERANCE_SHARD_H

#include <vector>

#include <kvcg_logging.h>
#include <kvcg_errors.h>

#include <networklayer/fabricBased.hh>
#include <faulttolerance/definitions.h>
#include <faulttolerance/backup_packet.h>

/**
 * @brief This object represents all connections which make up a shard
 */
class Shard {
public:
  /**
   * @brief Construct a new Shard object
   * 
   * @param lower_bound 
   * @param upper_bound 
   * @param hosts 
   */
  Shard(uint64_t lower_bound, uint64_t upper_bound, std::vector<char*> hosts) {
    this->key_range_.first = lower_bound;
    this->key_range_.second = upper_bound;

    for (char* host: hosts) {
      this->connections_.push_back(new cse498::FabricRPClient(host, cse498::DEFAULT_PORT));
    }
  }
  
  /**
   * @brief Destroy the Shard object
   */
  ~Shard() {
    for (cse498::FabricRPClient* conn : this->connections_) {
      delete conn;
    }
  }

  /**
   * @brief Check if a key would be managed by the shard
   * 
   * @param key the key to check
   * @return true if the key is within the range managed by this shard
   * @return false if the key is outside of the range managed by this shard
   */
  bool containsKey(key_t key) {
    return this->key_range_.first <= key && key <= this->key_range_.second;
  }
  
  int put(key_t key, data_t value) {
    BackupPacket<key_t,data_t> pkt(key, value);
    char* rawData = pkt.serialize();

    size_t dataSize = pkt.getPacketSize();
    // LOG(DEBUG4) << "raw data: " << (void*)rawData;
    // LOG(DEBUG4) << "data size: " << dataSize;

    // this->connections_.at(this->leader_index_)->wait_send(rawData, dataSize);

    return 0;
  }

  int get(key_t key, data_t* value) {
    return 0;
  }

private:
  size_t leader_index_ = 0;
  std::pair<key_t, key_t> key_range_;
  std::vector<cse498::FabricRPClient*> connections_;
};

#endif // FAULT_TOLERANCE_SHARD_H