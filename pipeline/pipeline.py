from dagster import repository, get_dagster_logger
from jobs.hn_launches_job import hn_launches_job, hn_launches_job_schedule
from jobs.growth_job import growth_job, growth_job_schedule


@repository
def hn_launches():
    
    return [hn_launches_job, hn_launches_job_schedule, growth_job, growth_job_schedule]
