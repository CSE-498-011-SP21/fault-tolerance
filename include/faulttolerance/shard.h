#ifndef FAULT_TOLERANCE_SHARD_H
#define FAULT_TOLERANCE_SHARD_H

#include <vector>
#include <faulttolerance/server.h>

class Shard {
private:
  std::vector<Server*> servers;
  Server* primary;
  std::pair<unsigned long long, unsigned long long> keyRange;

public:
  Shard(std::pair<unsigned long long, unsigned long long> kr) { keyRange = kr; }
  void addServer(Server* s) { servers.push_back(s); }
  Server* getPrimary() { return primary; }
  void setPrimary(Server* s) { primary = s; }
  void discoverPrimary() {};
  bool containsKey(unsigned long long key) { return keyRange.first <= key && key <= keyRange.second; };
  unsigned long long getLowerBound() { return keyRange.first; };
  unsigned long long getUpperBound() { return keyRange.second; };
};

#endif // FAULT_TOLERANCE_SHARD_H