#!/bin/bash

if [[ $(docker ps -a | grep -w dagster_c) ]]; then docker stop dagster_c && docker rm dagster_c; fi

if [[ $(docker images | grep -w dagster) ]]; then docker rmi dagster; fi

docker build -t dagster .

docker run --name dagster_c \
            -d \
            -p 8080:8080 \
            dagster
