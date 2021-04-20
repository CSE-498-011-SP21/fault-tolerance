#ifndef FAULT_TOLERANCE_NODE_H
#define FAULT_TOLERANCE_NODE_H

#include <string>
#include <kvcg_errors.h>
#include <networklayer/connection.hh>

// Forward declare Node in namespace
namespace cse498 {
  namespace faulttolerance {
    class Node;
  }
}

namespace ft = cse498::faulttolerance;

/**
 *
 * Base class for Server and Client
 *
 */
class ft::Node {
protected:
  std::string hostname;
  std::string addr = "";
  int clientPort;
  cse498::ProviderType provider;
  size_t cksum;

public:

  // Indicator flag if node is alive
  bool alive = true;

  /**
   *
   * Initialize node data
   *
   */
  virtual int initialize(std::string cfg_file) { return KVCG_ESUCCESS; }

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

  /**
   *
   * Get the client port of the node
   *
   * @return The client port of node
   *
   */
  int getClientPort() { return clientPort; }

  /**
   *
   * Set the client port of the node
   * 
   * @param p - port to set for clientPort
   *
   */
  void setClientPort(int port) { clientPort = port; }

  /**
   *
   * Get the provider of the node
   *
   * @return The provider of node
   *
   */
  cse498::ProviderType getProvider() { return provider; }

  /**
   *
   * Set the provider of the node
   * 
   * @param p - provider to set for provider
   *
   */
  void setProvider(cse498::ProviderType p) { provider = p; }

  bool operator < (const ft::Node& o) const { return hostname < o.hostname; }
};

#endif //FAULT_TOLERANCE_NODE_H
