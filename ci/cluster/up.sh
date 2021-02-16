#!/bin/bash

docker-compose up -d --scale node1=1 --scale node2=1 --no-recreate
