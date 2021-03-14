#ifndef FAULT_TOLERANCE_CLIENT_H
#define FAULT_TOLERANCE_CLIENT_H

#include <vector>

#include <kvcg_logging.h>
#include <kvcg_errors.h>

#include <faulttolerance/definitions.h>
#include <faulttolerance/node.h>
#include <faulttolerance/shard.h>

/**
 *
 * Client Node definition
 *
 */
class Client: public Node {
private:
  std::vector<Shard*> shard_list;
  
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

  int put(key_t key, data_t value);
  
  /**
   *
   * Get value in hash table on servers at key
   *
   * @param key - key to lookup in table
   *
   * @return value stored in table
   *
   */
  data_t get(key_t key);

  /**
   *
   * Get the primary server storing a key
   *
   * @param key - key whose primary server to search for
   *
   * @return Server storing key
   *
   */
  Shard* getShard(key_t key);
};

#endif //FAULT_TOLERANCE_CLIENT_H