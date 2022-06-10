#!/bin/bash

DOCKER_BUILD= "proof-Data-Engineer-SoniaC"

DOCKER_ID=$(docker ps -a -q --filter ancestor=${DOCKER_BUILD} --format="{{.ID}}")
sudo docker rm $(docker stop $(docker ps -a -q --filter ancestor=${DOCKER_BUILD} --format="{{.ID}}"))

sudo docker kill ${DOCKER_BUILD}
sudo docker rmi -f ${DOCKER_BUILD}
sudo docker rm -v -f ${DOCKER_BUILD}
sudo docker rmi $(docker images | grep ${DOCKER_BUILD})
sudo docker kill ${DOCKER_ID}
sudo docker rmi -f ${DOCKER_ID}
sudo docker rm -v -f ${DOCKER_ID}
sudo docker rmi $(docker images | grep ${DOCKER_ID})

sudo docker build -t proof-Data-Engineer-SoniaC .

#sudo docker run -td -p 6060:6060 ${DOCKER_BUILD}

DOCKER_ID=$(docker ps -a -q --filter ancestor=${DOCKER_BUILD} --format="{{.ID}}")

sudo docker exec ${DOCKER_ID} sh -c "echo The local IP is:"
sudo docker exec ${DOCKER_ID} sh -c "hostname -I"

sudo docker run -it ${DOCKER_ID} "../main.ipynb"
