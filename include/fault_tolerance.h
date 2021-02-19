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

enum LogLevel { ERROR, WARNING, INFO,
                DEBUG, DEBUG2, DEBUG3, DEBUG4 };
extern int LOG_LEVEL;

/**
 *
 * Initialize server
 *
 * @param None
 *
 * @return integer
 *
 */
int init_server();

/**
 *
 * Initialize client
 *
 * @param None
 *
 * @return integer
 *
 */
int init_client();

#endif // FAULT_TOLERANCE_H
