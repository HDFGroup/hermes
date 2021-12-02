//
// Created by jaime on 6/3/2021.
//

#ifndef HERMES_PUBSUB_H
#define HERMES_PUBSUB_H

/**
 * Standard header
 */

#include <mpi.h>

/**
 * Dependent library headers
 */
#include "glog/logging.h"

/**
 * Internal headers
 */
#include <bucket.h>
#include <hermes.h>

#include <hermes/adapter/pubsub/metadata_manager.h>
#include <hermes/adapter/singleton.h>
#include <hermes/adapter/pubsub/common/constants.h>

namespace hermes::pubsub{

hapi::Status mpiInit(int argc, char **argv);

hapi::Status connect(const std::string &config_file, bool independent = false);
hapi::Status connect(bool independent = false);
hapi::Status disconnect();

hapi::Status attach(const std::string& topic);
hapi::Status detach(const std::string& topic);

hapi::Status publish(const std::string& topic, const std::vector<unsigned char>& message);
std::pair<std::vector<unsigned char>, hapi::Status> subscribe(const std::string& topic);

}

#endif  // HERMES_PUBSUB_H
