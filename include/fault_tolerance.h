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
#include "kvcg_logging.h"

extern std::string CFG_FILE;

// Base class
class Node {
public:
  virtual int initialize() { return 0; }
};

class Server: public Node {
private:
  bool primary;
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
   * Check if server is running as primary
   * 
   * @param None
   *
   * @return bool - true if primary, false otherwise
   *
   */
  bool isPrimary() { return primary; }
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
