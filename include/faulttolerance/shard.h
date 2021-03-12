/****************************************************
 *
 * Shard Class Definition
 *
 ****************************************************/

#ifndef FAULT_TOLERANCE_SHARD_H
#define FAULT_TOLERANCE_SHARD_H

#include <vector>

#include <networklayer/connection.hh>

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
      this->connections_.push_back(new cse498::Connection(host));
    }
  }
  
  /**
   * @brief Destroy the Shard object
   */
  ~Shard() {
    for (cse498::Connection* conn : this->connections_) {
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
  bool containsKey(uint64_t key) {
    return this->key_range_.first <= key && key <= this->key_range_.second;
  }

  /**
   * @brief Get the connection object for the leader of this shard
   * 
   * @return cse498::Connection* 
   */
  cse498::Connection* getLeaderConn() {
    return connections_.at(this->leader_index_);
  }

private:
  size_t leader_index_ = 0;
  std::pair<u_int64_t, uint64_t> key_range_;
  std::vector<cse498::Connection*> connections_;
};

#endif // FAULT_TOLERANCE_SHARD_H