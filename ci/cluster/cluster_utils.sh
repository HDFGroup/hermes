#!/bin/bash

set -x -e

node_names=($(awk '/hostname:/ {print $2}' docker-compose.yml))
docker_user=mpirun
docker_home=/home/${docker_user}
script_dir="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
hermes_dir=${script_dir}/../..
cluster_conf=${hermes_dir}/test/data/hermes_server_ci.yaml
hermes_build_dir=${hermes_dir}/build
project_name="$(basename ${script_dir})"
host1=${project_name}_${node_names[0]}_1
host2=${project_name}_${node_names[1]}_1


# Build images and start a cluster
function hermes_cluster_up() {
    local num_workers=${1:-1}

    # Build the images, passing our user id and group id so the container user can
    # modify the .gcda coverage files
    for n in "${node_names[@]}"; do
        docker-compose build --build-arg GROUP_ID=$(id -g) --build-arg USER_ID=$(id -u) ${n}
    done

    docker-compose up -d --scale ${node_names[0]}=1 --scale ${node_names[1]}=${num_workers} --no-recreate

    for h in ${host1} ${host2}; do
        # Create the hosts file
        docker exec --user ${docker_user} -w ${hermes_build_dir} ${h} \
               bash -c "echo -e \"${host1}\n${host2}\n\" > hermes_hosts"
        # Copy ssh keys to ${docker_home}/.ssh
        docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa ${docker_home}/.ssh/id_rsa"
        docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa.pub ${docker_home}/.ssh/id_rsa.pub"
        docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa.pub ${docker_home}/.ssh/authorized_keys"
        docker exec ${h} chown -R ${docker_user}:${docker_user} ${docker_home}/.ssh
        # Create the data directory
        docker exec ${h} bash -c "mkdir /tmp/test_hermes"
    done
}

function hermes_cluster_test() {
    local allocate_tty=${1:-}
    local hosts=${host1},${host2}

    docker-compose exec ${allocate_tty}                                \
                   -e GLOG_vmodule=rpc_thallium=1                      \
                   -e LSAN_OPTIONS=suppressions=../test/data/asan.supp \
                   --user ${docker_user}                               \
                   -w ${hermes_build_dir}                              \
                   ${node_names[0]}                                    \
                   echo `mpirun -n 2 -ppn 1 -hosts ${hosts} bin/hermes_daemon ${cluster_conf} &` \
                   && sleep 3 \
                   && mpirun -n 4 -ppn 2 -hosts ${hosts} -genv ${cluster_conf} bin/test_multinode_put_get
}

# Stop the cluster
function hermes_cluster_down() {
    docker-compose down
}
