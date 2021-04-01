/****************************************************
 *
 * Config File Parser
 *
 ****************************************************/


#ifndef KVCG_CONFIG_H
#define KVCG_CONFIG_H

#include <string>
#include <iostream>
#include <sstream>
#include <kvcg_logging.h>
#include <networklayer/connection.hh>
#include <faulttolerance/server.h>

/**
 *
 * Class to parse config file and store data
 *
 */
class KVCGConfig {
private:
  std::vector<Server*> serverList;
  cse498::ProviderType provider;

public:
  /**
   *
   * Parse JSON input file
   *
   * @param filename - name of JSON file to parse
   *
   * @return status. 0 on success, non-zero otherwise.
   *
   */
  int parse_json_file(std::string filename);

  /**
   *
   * Calculate and return a checksum for the configuration.
   *
   * @return hash of config file
   *
   */
  std::size_t get_checksum();

  /**
   *
   * Get list of servers parsed from config.
   *
   * @return vector of Servers
   *
   */
  std::vector<Server*> getServerList() { return serverList; }

  /**
   *
   * Get the provider from config.
   *
   * @return ProviderType for servers.
   *
   */
  cse498::ProviderType getProvider() { return provider; }

};

#endif // KVCG_CONFIG_H
