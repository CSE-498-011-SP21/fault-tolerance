#ifndef FAULT_TOLERANCE_CLIENT_H
#define FAULT_TOLERANCE_CLIENT_H

#include <vector>

#include <kvcg_logging.h>
#include <kvcg_errors.h>
#include <data_t.hh>

#include <faulttolerance/node.h>
#include <faulttolerance/server.h>
#include <faulttolerance/shard.h>
#include <faulttolerance/kvcg_config.h>

// Forward declare Client in namespace
namespace cse498 {
  namespace faulttolerance {
    class Client;
  }
}

namespace ft = cse498::faulttolerance;

/**
 *
 * Client Node definition
 *
 */
class ft::Client: public ft::Node {
private:
  std::vector<ft::Shard*> shardList;
  std::vector<ft::Server*> serverList;
  cse498::unique_buf rawBuf(4096); // TODO: insert variable in place of raw 4096? need to register mr somewhere ???

public:
  /**
   *
   * Initialize client
   *
   * @return status. 0 on success, non-zero otherwise.
   *
   */
  int initialize(std::string cfg_file);

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
  int put(unsigned long long key, data_t* value);

  /**
   *
   * Get value in hash table on servers at key
   *
   * @param key - key to lookup in table
   *
   * @return value stored in table
   *
   */
  data_t* get(unsigned long long key);

  /**
   *
   * Get the shard storing the key
   *
   * @param key - key whose shard to search for
   *
   * @return shard storing key
   *
   */
  ft::Shard* getShard(unsigned long long key);

  /**
   *
   * Get the new primary server storing a key by broadcasting to the servers in a shard
   *
   * @param shard - shard whose servers to broadcast to
   *
   * @return new primary server storing the key
   *
   */
  ft::Server* getPrimaryOnFailure(unsigned long long key, Shard* shard);
};

#endif //FAULT_TOLERANCE_CLIENT_H
