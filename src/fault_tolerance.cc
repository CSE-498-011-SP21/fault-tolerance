/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>
#include <boost/asio/ip/host_name.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

namespace pt = boost::property_tree;

// set defaults
std::string CFG_FILE = "./kvcg.json";
int LOG_LEVEL = INFO;

BackupPacket::BackupPacket(char* rawData) {
  // Process raw bytes into a struct
  memcpy(&this->key, rawData, sizeof(this->key));
  memcpy(&this->valueSize, rawData+sizeof(this->key), sizeof(this->valueSize));
  memcpy(&this->value, rawData+sizeof(this->key)+sizeof(this->valueSize), this->valueSize);
}

BackupPacket::BackupPacket(int key, size_t valueSize, char* value) {
    this->key = key;
    this->valueSize = valueSize;
    this->value = value;
}

char* BackupPacket::serialize() {
    char* rawData = (char*) malloc(sizeof(key)+sizeof(valueSize)+valueSize);
    memcpy(rawData, &this->key, sizeof(key));
    memcpy(rawData+sizeof(key), &this->valueSize, sizeof(valueSize));
    memcpy(rawData+sizeof(key)+sizeof(valueSize), value, valueSize);
    return rawData;
}

