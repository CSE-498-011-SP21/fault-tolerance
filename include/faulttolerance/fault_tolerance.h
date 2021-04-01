/****************************************************
 *
 * Fault Tolerance API Definitions
 *
 ****************************************************/

#ifndef FAULT_TOLERANCE_H
#define FAULT_TOLERANCE_H

/**
 * @file
 *
 * @brief Public API for KVCG Fault Tolerance protocol
 *
 */
#include <cstring>
#include <thread>
#include <string>
#include <vector>
#include <boost/range/combine.hpp>
#include <unistd.h>
#include <kvcg_logging.h>
#include <kvcg_errors.h>
#include <faulttolerance/backup_packet.h>
#include <faulttolerance/node.h>
#include <faulttolerance/server.h>
#include <faulttolerance/client.h>

extern std::string CFG_FILE;

#define SERVER_PORT 8080
#define CLIENT_PORT 8081

#endif //FAULT_TOLERANCE_H
