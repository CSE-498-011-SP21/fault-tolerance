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
#include <string>
#include <vector>
#include "kvcg_logging.h"

extern std::string CFG_FILE;

// Base class
class Node {
protected:
  std::string hostname;
public:
  virtual int initialize() { return 0; }
  void setName(std::string n) { hostname = n; }
  std::string getName() { return hostname; }
};

class Server: public Node {
private:
  std::vector<std::pair<int, int>> primaryKeys; // primary key ranges

  std::vector<Server*> backupServers; // servers backing up this ones primaries
  std::vector<Server*> primaryServers; // servers whose keys this one is backing up
public:

  /**
   *
   * Initialize server
   *
   * @param None
   *
   * @return integer
   *
   */
  int initialize();

  /**
   *
   * Get vector of primary key ranges
   *
   * @param None
   *
   * @return vector of min/max key range pairs
   *
   */
  std::vector<std::pair<int, int>> getPrimaryKeys() { return primaryKeys; }

  /**
   *
   * Add key range to primary list
   *
   * @param keyRange - std::pair<int, int> of min and max key
   *
   * @return bool - true if added successfully, false otherwise
   *
   */
  bool addKeyRange(std::pair<int, int> keyRange);

  /**
   *
   * Add server who this one is backing up
   *
   * @param s - Server to add to primary server list
   *
   * @return bool - true if added successfully, false otherwise
   *
   */
  bool addPrimaryServer(Server* s);

  /**
   *
   * Add server who is backing this one up
   *
   * @param s - Server to add to backup server list
   *
   * @return bool - true if added successfully, false otherwise
   *
   */
  bool addBackupServer(Server* s);

  /**
   * 
   * Check if server is running as primary
   * for a given key
   * 
   * @param None
   *
   * @return bool - true if primary, false otherwise
   *
   */
  bool isPrimary(int key);

  /**
   *
   * Check if server is running as backup
   * for a given key
   *
   * @param None
   *
   * @return bool - true if backup, false otherwise
   *
   */
};

class Client: public Node {
public:
  /**
   *
   * Initialize client
   *
   * @param None
   *
   * @return integer
   *
   */
  int initialize();
};

#endif // FAULT_TOLERANCE_H
