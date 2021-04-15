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
  Shard(std::pair<unsigned long long, unsigned long long> kr) { keyRange = kr; }
  void addServer(ft::Server* s) { servers.push_back(s); }
  ft::Server* getPrimary() { return primary; }
  void setPrimary(ft::Server* s) { primary = s; }
  void discoverPrimary() {};
  bool containsKey(unsigned long long key) { return keyRange.first <= key && key <= keyRange.second; };
  unsigned long long getLowerBound() { return keyRange.first; };
  unsigned long long getUpperBound() { return keyRange.second; };
  std::vector<ft::Server*> getServers() { return servers; }
};

#endif // FAULT_TOLERANCE_SHARD_H
