#!/bin/bash

set -x -e

node_names=($(awk '/hostname:/ {print $2}' docker-compose.yml))
docker_user=mpirun
docker_home=/home/${docker_user}
cluster_conf=${docker_home}/hermes.yaml
script_dir="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
hermes_build_dir=${script_dir}/../../build
project_name="$(basename ${script_dir})"
host1=${project_name}_${node_names[0]}_1
host2=${project_name}_${node_names[1]}_1


# Build images and start a cluster
function hermes_cluster_up() {
    local num_workers=${1:-1}
    local conf_path=${script_dir}/../../test/data/hermes.yaml

    # Build the images, passing our user id and group id so the container user can
    # modify the .gcda coverage files
    for n in "${node_names[@]}"; do
        docker-compose build --build-arg GROUP_ID=$(id -g) --build-arg USER_ID=$(id -u) ${n}
    done

    docker-compose up -d --scale ${node_names[0]}=1 --scale ${node_names[1]}=${num_workers} --no-recreate

    for h in ${host1} ${host2}; do
        # Change the default hermes.yaml file to accommodate multiple nodes and
        # store it at ${cluster_conf} on each node.
        # 1. Replace "./" mount_points and swap_mount with ${docker_home}
        # 2. Use rpc_server_host_file
        # 3. Change num_rpc_threads to 4
        docker exec --user ${docker_user} -w ${hermes_build_dir} ${h} \
               bash -c "sed -e 's|\"\./\"|\""${docker_home}"\"|g' \
                        -e 's|rpc_server_host_file = \"\"|rpc_server_host_file = \"hermes_hosts\"|' \
                        -e 's|rpc_num_threads = 1|rpc_num_threads = 4|' \
                        ${conf_path} > ${cluster_conf}"

        # Create the hosts file
        docker exec --user ${docker_user} -w ${hermes_build_dir} ${h} \
               bash -c "echo -e \"${host1}\n${host2}\n\" > hermes_hosts"
        # Copy ssh keys to ${docker_home}/.ssh
        docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa ${docker_home}/.ssh/id_rsa"
        docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa.pub ${docker_home}/.ssh/id_rsa.pub"
        docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa.pub ${docker_home}/.ssh/authorized_keys"
        docker exec ${h} chown -R ${docker_user}:${docker_user} ${docker_home}/.ssh
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
                   mpirun -n 4 -ppn 2 -hosts ${hosts} bin/end_to_end_test ${cluster_conf}
}

# Stop the cluster
function hermes_cluster_down() {
    docker-compose down
}
