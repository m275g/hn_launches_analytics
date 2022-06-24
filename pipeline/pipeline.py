from dagster import repository, get_dagster_logger
from jobs.hn_launches_job import hn_launches_job, hn_launches_schedule
from jobs.growjo_job import growjo_job, growjo_job_schedule


@repository
def hn_launches():
    
    return [hn_launches_job, hn_launches_schedule, growjo_job, growjo_job_schedule]