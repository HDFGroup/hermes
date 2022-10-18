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

#ifndef HERMES_PUBSUB_H
#define HERMES_PUBSUB_H

/**
 * Standard header
 */

#include <mpi.h>

/**
 * Dependent library headers
 */
#include <glog/logging.h>

/**
 * Internal headers
 */
#include <bucket.h>
#include <hermes.h>
#include "metadata_manager.h"
#include "../constants.h"
#include "singleton.h"

namespace hermes::pubsub {

/**
 * \brief Helper function to initialize MPI
 *
 * \return The return code/status
 *
 */
hapi::Status mpiInit(int argc, char **argv);

/**
 * \brief Connects to the Hermes instance
 *
 * \param config_file Path to the config file of Hermes
 *
 * \return The return code/status
 *
 */
hapi::Status connect(const std::string &config_file);

/**
 * \brief Connects to the Hermes instance
 *
 * \return The return code/status
 *
 * \pre Assumes that the config file path is loaded into a environment variable
 * defined in constants.h under kHermesConf
 *
 */
hapi::Status connect();

/**
 * \brief Connects from the Hermes instance
 *
 * \return The return code/status
 */
hapi::Status disconnect();

/**
 * \brief Attaches to a topic, creating it if it doesnt exists.
 *
 * \param topic The name of the topic
 *
 * \return The return code/status
 *
 */
hapi::Status attach(const std::string& topic);

/**
 * \brief Detaches from the topic. Cleaning up all metadata
 *
 * \param topic The name of the topic
 *
 * \return The return code/status
 *
 * \pre Detaching doesnt delete the topic
 *
 */
hapi::Status detach(const std::string& topic);

/**
 * \brief Puts a message to a topic
 *
 * \param topic The name of the topic
 * \param message the data buffer
 *
 * \return The return code/status
 *
 * \remark Using std::vector<unsigned char> as equivalent to Blob
 *
 */
hapi::Status publish(const std::string& topic,
                     const std::vector<unsigned char>& message);

/**
 * \brief Retrieves the next message from the topic
 *
 * \param topic The name of the topic
 *
 * \return A pair of:
 *         the return code/status
 *         and, if successful, the subscribed message.
 *
 * \remark Message order is tracked by Hermes. Current message is tracked
 * per-process by a metadata manager
 *
 */
std::pair<std::vector<unsigned char>, hapi::Status> subscribe(
    const std::string& topic);

}  // namespace hermes::pubsub

#endif  // HERMES_PUBSUB_H
