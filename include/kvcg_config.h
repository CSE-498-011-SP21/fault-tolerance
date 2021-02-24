/****************************************************
 *
 * Config File Parser
 *
 ****************************************************/


#ifndef KVCG_CONFIG_H
#define KVCG_CONFIG_H

#include <iostream>
#include <sstream>
#include "fault_tolerance.h"
#include "kvcg_logging.h"

class KVCGConfig {
public:
  int parse_json_file(std::string filename);
  std::vector<Server*> serverList;
};

#endif // KVCG_CONFIG_H
