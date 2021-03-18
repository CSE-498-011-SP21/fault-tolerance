#ifndef FAULT_TOLERANCE_SHARD_H
#define FAULT_TOLERANCE_SHARD_H

#include <vector>
#include <faulttolerance/server.h>

class Shard {
private:
  std::vector<Server*> servers;
  Server* primary;
  std::pair<uint64_t, uint64_t> keyRange;

public:
  Shard(std::pair<uint64_t, uint64_t> kr) { keyRange = kr; }
  void addServer(Server* s) { servers.push_back(s); }
  Server* getPrimary(bool force) { return primary; }
  void setPrimary(Server* s) { primary = s; }
  bool inKeyRange(uint64_t key) { return keyRange.first <= key && keyRange.second >= key; }
  std::vector<Server*> getServers() { return servers; }
};

#endif // FAULT_TOLERANCE_SHARD_H