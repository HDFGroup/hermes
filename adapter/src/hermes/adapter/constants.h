/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

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
 * kHermesBufferMode env variable is used to define the mode in which Hermes'
 * STDIO adapter operates.
 *
 * It supports 3 Values: PERSISTENT/BYPASS/SCRATCH
 */
const char* kHermesBufferMode = "HERMES_BUFFERING_MODE";
/**
 * kHermesBufferModeInfo env variable is used to define the additional
 * information for the selected mode @see kHermesBufferMode.
 *
 * Currently it supports info for only BYPASS mode where users can define which
 * files to bypass.
 */
const char* kHermesBufferModeInfo = "HERMES_BUFFERING_MODE_INFO";
/**
 * Path delimiter used for defining multiple file paths in Hermes.
 */
const char kHermesPathDelimiter = ',';
/**
 * Constants for supported buffering modes in Hermes
 */
const char* kPersistentHermesBufferMode = "PERSISTENT";
const char* kBypassHermesBufferMode = "BYPASS";
const char* kScratchHermesBufferMode = "SCRATCH";
#endif  // HERMES_ADAPTER_CONSTANTS_H
