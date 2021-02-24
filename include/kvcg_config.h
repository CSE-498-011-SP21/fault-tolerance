/****************************************************
 *
 * Config File Parser
 *
 ****************************************************/


#ifndef KVCG_CONFIG_H
#define KVCG_CONFIG_H

#include <iostream>
#include <sstream>
#include "kvcg_logging.h"
#include "fault_tolerance.h"

class KVCGConfig {
public:
  int parse_json_file(std::string filename);
  std::size_t get_checksum();
  std::vector<Server*> serverList;
  std::vector<Server*> getServerList() { return serverList; }
};

#endif // KVCG_CONFIG_H
