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
  std::string addr = "";

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
   * Set the address of the node
   *
   * @param a - Address to set for node
   *
   */
  void setAddr(std::string a) { addr = a; }

  /**
   *
   * Get the name of the node
   *
   * @return Name of the node
   *
   */
  std::string getName() { return hostname; }

  /**
   *
   * Get the address of the node
   *
   * @return Addres of node
   *
   */
  std::string getAddr() { return addr; }

  bool operator < (const Node& o) const { return hostname < o.hostname; }
};

#endif //FAULT_TOLERANCE_NODE_H
