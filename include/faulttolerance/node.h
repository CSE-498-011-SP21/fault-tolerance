#ifndef FAULT_TOLERANCE_NODE_H
#define FAULT_TOLERANCE_NODE_H

#include <string>
#include <kvcg_errors.h>

/**
 *
 * Base class for Server and Client
 *
 */
class Node {
protected:
  std::string hostname;

public:

  // Indicator flag if node is alive
  bool alive = true;

  /**
   *
   * Initialize node data
   *
   */
  virtual int initialize() { return KVCG_ESUCCESS; }

  /**
   *
   * Set the name of the node
   *
   * @param n - Name to set for node
   *
   */
  void setName(std::string n) { hostname = n; }

  /**
   *
   * Get the name of the node
   *
   * @return Name of the node
   *
   */
  std::string getName() { return hostname; }

  bool operator < (const Node& o) const { return hostname < o.hostname; }
};

#endif //FAULT_TOLERANCE_NODE_H