from dagster import repository, get_dagster_logger
from jobs.hn_launches_job import hn_launches_job, hn_launches_schedule
from jobs.growth_job import growth_job, growth_job_schedule


@repository
def hn_launches():
    
    return [hn_launches_job, hn_launches_schedule, growth_job, growth_job_schedule]


#!/bin/bash

if [[ $(docker ps -a | grep -w dagster_c) ]]; then docker stop dagster_c && docker rm dagster_c; fi

if [[ $(docker images | grep -w dagster) ]]; then docker rmi dagster; fi

docker build -t dagster .

docker run --name dagster_c \
            -d \
            -p 8080:8080 \
            -v $(pwd)/data:/opt/dagster/app/ \
            dagster