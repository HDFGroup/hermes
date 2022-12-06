//
// Created by lukemartinlogan on 12/1/22.
//

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
