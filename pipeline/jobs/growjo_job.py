from ops.parse_load_growjo import parse_load_growjo
from dagster import op, Out, In, graph, job, repository, schedule, ScheduleDefinition, get_dagster_logger


@job()
def growjo_job():
    
    parse_load_growjo()
    
@schedule(job = growjo_job,
          cron_schedule = "@daily",
          execution_timezone = "Europe/Moscow",
          default_status = DefaultScheduleStatus.RUNNING)
def growjo_job_schedule(context):
    return {}