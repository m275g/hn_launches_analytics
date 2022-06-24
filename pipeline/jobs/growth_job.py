from ops.parse_growjo import parse_growjo
from ops.parse_github_stats import parse_github

from dagster import op, Out, In, graph, job, repository, schedule, ScheduleDefinition, get_dagster_logger


@job()
def growth_job():
    
    transform_load_growth(parse_growjo(), parse_github())
    
@schedule(job = growth_job,
          cron_schedule = "@daily",
          #execution_timezone = "Europe/Moscow",
          default_status = DefaultScheduleStatus.RUNNING)
def growth_job_schedule(context):
    return {}