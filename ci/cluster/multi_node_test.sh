#!/bin/bash

set -x -e

docker_user=mpirun
docker_home=/home/${docker_user}
conf_path=${GITHUB_WORKSPACE}/test/data/hermes.conf
cluster_conf=${docker_home}/hermes.conf
project_name="$(basename $(cd $(dirname "${BASH_SOURCE[0]}") && pwd))"
node_names=(node1 node2)
host1=${project_name}_${node_names[0]}_1
host2=${project_name}_${node_names[1]}_1
hosts=${host1},${host2}
hermes_build_dir=${GITHUB_WORKSPACE}/build

# Start the cluster
docker-compose up -d --scale ${node_names[0]}=1 --scale ${node_names[1]}=1 --no-recreate

for h in ${host1} ${host2}; do
    # Change the default hermes.conf file to accommodate multiple nodes and
    # store it at ${cluster_conf} on each node.
    # 1. Replace "./" mount_points and swap_mount with ${docker_home}
    # 2. Change rpc_server_base_name to 'node'
    # 3. Change num_rpc_threads to 4
    # 4. Change rpc_host_number_range to {1, 2}
    docker exec --user ${docker_user} -w ${hermes_build_dir} ${h} \
           bash -c "sed -e 's|\"\./\"|\""${docker_home}"\"|g' \
                        -e 's|\"localhost\"|\"node\"|' \
                        -e 's|rpc_num_threads = 1|rpc_num_threads = 4|' \
                        -e 's|{0, 0}|{1, 2}|' ${conf_path} > ${cluster_conf}"

    # Copy ssh keys to ${docker_home}/.ssh
    docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa ${docker_home}/.ssh/id_rsa"
    docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa.pub ${docker_home}/.ssh/id_rsa.pub"
    docker exec ${h} bash -c "cp ${HOME}/.ssh/id_rsa.pub ${docker_home}/.ssh/authorized_keys"
    docker exec ${h} chown -R ${docker_user}:${docker_user} ${docker_home}/.ssh
done



# Run the Hermes tests on the cluster
# TODO: Send the output of this to our Github action
docker-compose exec -e GLOG_vmodule=rpc_thallium=1 \
               -e LSAN_OPTONS=suppressions=../test/data/asan.supp \
               --user ${docker_user} --privileged \
               -w ${hermes_build_dir} ${node_names[0]} \
               mpirun -n 4 -ppn 2 -hosts ${hosts} bin/end_to_end_test ${cluster_conf}

# Stop the cluster
docker-compose down --rmi

