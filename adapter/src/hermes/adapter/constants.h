#ifndef HERMES_ADAPTER_CONSTANTS_H
#define HERMES_ADAPTER_CONSTANTS_H

/**
 * Constants file for all adapter.
 * This file contains common constants across different adapters.
 */

/**
 * HERMES_CONF env variable is used to define path to HERMES_CONF in adapters.
 * This is used for initialization of Hermes.
 */
const char* HERMES_CONF = "HERMES_CONF";
/**
 * HERMES_EXT is the extension of hermes files. These are buffered files from
 * I/O. This is needed as we should not intercept these files.
 */
const char* HERMES_EXT = ".hermes";

#endif  // HERMES_ADAPTER_CONSTANTS_H
