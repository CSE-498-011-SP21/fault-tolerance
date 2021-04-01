#include <faulttolerance/fault_tolerance.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace pt = boost::property_tree;

std::string CFG_FILE = "./kvcg.json";
int LOG_LEVEL = INFO;