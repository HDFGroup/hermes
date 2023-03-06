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

#ifndef HERMES_SRC_RPC_GENERATOR_DECORATOR_H_
#define HERMES_SRC_RPC_GENERATOR_DECORATOR_H_

/**
 * Decorators for RPC code generation
 *
 * code_generators/rpc.py
 * */
#define RPC
#define RPC_AUTOGEN_START
#define RPC_AUTOGEN_END
#define RPC_CLASS_INSTANCE_DEFS_START
#define RPC_CLASS_INSTANCE_DEFS_END

/**
 * In many cases, we define functions which have a large parameter
 * set and then produce wrapper functions which set those parameters
 * differently. These decorators automate generating those wrapper
 * functions
 * */

#define WRAP(...)
#define WRAP_AUTOGEN_START
#define WRAP_AUTOGEN_END

#endif  // HERMES_SRC_RPC_GENERATOR_DECORATOR_H_
