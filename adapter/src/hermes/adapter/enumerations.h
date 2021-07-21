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

#ifndef HERMES_ADAPTER_ENUMERATIONS_H
#define HERMES_ADAPTER_ENUMERATIONS_H
enum AdapterMode {
  kDefault = 0, /* all/given files are stored on file close or flush. */
  kBypass = 1, /* all/given files are not buffered. */
  kScratch = 2 /* all/given files are ignored on file close or flush. */
};

enum class FlushingMode {
  kSynchronous,
  kAsynchronous,
};

#endif  // HERMES_ADAPTER_ENUMERATIONS_H
