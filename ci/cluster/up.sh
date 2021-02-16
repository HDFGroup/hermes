#!/bin/bash

for n in node1 node2; do
    docker-compose build --build-arg GROUP_ID=$(id -g) --build-arg USER_ID=$(id -u) ${n}
done

docker-compose up -d --scale node1=1 --scale node2=1 --no-recreate
