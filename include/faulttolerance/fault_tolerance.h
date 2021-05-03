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
#include <faulttolerance/node.h>
#include <faulttolerance/server.h>
#include <faulttolerance/shard.h>
#include <faulttolerance/client.h>


namespace ft = cse498::faulttolerance;

namespace cse498 {
  namespace faulttolerance {
/**
 * unfold a number of requests into a serialized array of RequestWrapper objects
 * 
 * @param keys 
 * @param prevValues 
 * @param requestTypes 
 * @param newValues 
 * @return std::vector<RequestWrapper<unsigned long long, data_t *>> 
 */
std::vector<RequestWrapper<unsigned long long, data_t *>> unfoldRequest(
    std::vector<unsigned long long> keys,
    std::vector<data_t *> prevValues,
    std::vector<data_t *> newValues,
    std::vector<unsigned> requestTypes);
  }
}

#endif //FAULT_TOLERANCE_H
