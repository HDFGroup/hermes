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

#ifndef HERMES_SRC_LOGGING_H_
#define HERMES_SRC_LOGGING_H_

/** Logging levels */
#define kDebug 10  /**< Low-priority debugging information*/
// ... may want to add more levels here
#define kInfo 0    /**< Useful information the user should know */

/** Change the amount of logs shown at compile time */
#ifdef HERMES_LOG_VERBOSITY
#define VLOG_LEVEL HERMES_LOG_VERBOSITY
#endif

#include "glog/logging.h"

#endif  // HERMES_SRC_LOGGING_H_
