//
// Created by jaime on 6/3/2021.
//

#ifndef HERMES_PUBSUB_H
#define HERMES_PUBSUB_H

#include <bucket.h>
#include <hermes.h>
#include <hermes_status.h>

#include <hermes/adapter/pubsub/metadata_manager.h>
#include <hermes/adapter/singleton.h>
#include <hermes/adapter/pubsub/common/constants.h>

#include <mpi.h>

namespace hermes::pubsub{

hapi::Status mpiInit(int argc, char **argv);

hapi::Status connect(const char *config_file);
hapi::Status connect();
hapi::Status disconnect();

hapi::Status attach(const std::string& topic);
hapi::Status detach(const std::string& topic);

hapi::Status publish(const std::string& topic, const std::vector<unsigned char>& message);
std::pair<std::vector<unsigned char>, hapi::Status> subscribe(const std::string& topic);

}

#endif  // HERMES_PUBSUB_H
