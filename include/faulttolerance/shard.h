#ifndef FAULT_TOLERANCE_SHARD_H
#define FAULT_TOLERANCE_SHARD_H

#include <vector>
#include <faulttolerance/server.h>

// Forward declare Shard in namespace
namespace cse498 {
  namespace faulttolerance {
    class Shard;
  }
}

namespace ft = cse498::faulttolerance;

class ft::Shard {
private:
  std::vector<ft::Server*> servers;
  ft::Server* primary;
  std::pair<unsigned long long, unsigned long long> keyRange;

public:
  // Initialize shard with given key range
  Shard(std::pair<unsigned long long, unsigned long long> kr) { keyRange = kr; }

  /**
   *
   * Add a server to this Shard
   *
   * @param s - server to add
   *
   */
  void addServer(ft::Server* s) { servers.push_back(s); }

  /**
   *
   * Get the cached primary for this Shard
   *
   * @return current primary server
   *
   */
  ft::Server* getPrimary() { return primary; }

  /**
   *
   * Set the current primary in this Shard's cached data
   *
   * @param s - Server pointer to primary
   *
   */
  void setPrimary(ft::Server* s) { primary = s; }

  /**
   *
   * Determine if this Shard contains a given key
   *
   * @param key - key to check in Shard
   *
   * @return true if contains key, false otherwise
   *
   */
  bool containsKey(unsigned long long key) {
      return keyRange.first <= key && key <= keyRange.second;
  };

  /**
   *
   * Get the lower bound on this Shard's key range
   *
   * @return lower key value
   *
   */
  unsigned long long getLowerBound() { return keyRange.first; };

  /**
   *
   * Get the upper bound on this Shard's key range
   *
   * @return upper key value
   *
   */
  unsigned long long getUpperBound() { return keyRange.second; };

  /**
   *
   * Get the list of servers in this Shard
   *
   * @return list of servers
   *
   */
  std::vector<ft::Server*> getServers() { return servers; }
};

#endif // FAULT_TOLERANCE_SHARD_H
