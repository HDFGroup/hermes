#!/bin/bash

set -x
set -e
set -o pipefail

# Pull the Hermes dependencies image
docker pull lukemartinlogan/hermes_deps:latest
docker run -d \
--mount src=${PWD},target=/hermes,type=bind \
--name hermes_deps_c \
--network host \
lukemartinlogan/hermes_deps \
tail -f /dev/null
