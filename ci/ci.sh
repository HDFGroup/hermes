sudo docker build -t hermes-ci `pwd`/docker/ -f `pwd`/docker/ctest.Dockerfile
sudo docker image rm hermes-ci