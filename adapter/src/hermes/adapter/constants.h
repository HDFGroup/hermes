#ifndef HERMES_ADAPTER_CONSTANTS_H
#define HERMES_ADAPTER_CONSTANTS_H

/**
 * Constants file for all adapter.
 * This file contains common constants across different adapters.
 */

/**
 * kHermesConf env variable is used to define path to kHermesConf in adapters.
 * This is used for initialization of Hermes.
 */
const char* kHermesConf = "HERMES_CONF";
/**
 * kHermesExtension is the extension of hermes files. These are buffered files
 * from I/O. This is needed as we should not intercept these files.
 */
const char* kHermesExtension = ".hermes";
/**
 * Log file extensions
 */
const char* kLogExtension = ".log";

#endif  // HERMES_ADAPTER_CONSTANTS_H
