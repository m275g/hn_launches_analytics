from dagster import repository, get_dagster_logger
from jobs.hn_launches_job import hn_launches_job
from jobs.growjo_job import growjo_job


@repository
def hn_launches():
    
    return [hn_launches_job, growjo_job]