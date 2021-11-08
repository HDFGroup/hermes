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
 * Environment variable used to inform Hermes if this run consists of a separate
 * client that need to attach to an existing daemon (1) or if this run is
 * co-deployed with a Hermes core (0). This is only relevant for Hermes jobs
 * launched with 1 application process, as Hermes jobs run with >1 MPI ranks
 * require a daemon. Defaults to 0.
 */
const char* kHermesClient = "HERMES_CLIENT";

/**
 * Specifies whether or not Hermes should eagerly and asynchronously flush
 * writes to their final destinations.
 *
 * When set to "1", each write will first Put the Blob into the buffering
 * system, then trigger a background job to flush the Blob to its file. This is
 * useful for write-only workloads. If "0" or unset, normal flushing occurs when
 * fclose is intercepted.
 */
const char* kHermesAsyncFlush = "HERMES_ASYNC_FLUSH";

/**
 * kHermesExtension is the extension of hermes files. These are buffered files
 * from I/O. This is needed as we should not intercept these files.
 */
const char* kHermesExtension = ".hermes";
/**
 * ADAPTER_MODE env variable is used to define the mode in which Hermes'
 * STDIO adapter operates.
 *
 * It supports 3 Values: DEFAULT/BYPASS/SCRATCH
 *
 * DEFAULT: by default adapter operates in Persistent mode.
 * BYPASS: the adapter will bypass hermes.
 * SCRATCH: the adapter will discard the data operation on close.
 */
const char* kAdapterMode = "ADAPTER_MODE";
/**
 * ADAPTER_MODE_INFO env variable is used to define the additional
 * information for the selected mode @see kAdapterMode.
 *
 * for DEFAULT: which files should be persisted.
 * for BYPASS: which files should be bypassed.
 * for SCRATCH: which files will be opened in scratch model.
 */
const char* kAdapterModeInfo = "ADAPTER_MODE_INFO";
/**
 * Path delimiter used for defining multiple file paths in Hermes' Adapter.
 */
const char kPathDelimiter = ',';
/**
 * Constants for supported buffering modes in Hermes
 */
const char* kAdapterDefaultMode = "DEFAULT";
const char* kAdapterBypassMode = "BYPASS";
const char* kAdapterScratchMode = "SCRATCH";

/**
 * If the \c HERMES_STOP_DAEMON environment variable is unset or has a non-zero
 * value, the adapter client will kill the running Hermes daemon when it
 * finishes execution.
 *
 * Default value: \c 1
 */
const char* kStopDaemon = "HERMES_STOP_DAEMON";
#endif  // HERMES_ADAPTER_CONSTANTS_H
