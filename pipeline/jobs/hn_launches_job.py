from ops.parse_hn_launches import parse_hn_launches
from ops.transform_load_hn_launches import transform_load_hn_launches
from dagster import op, Out, In, graph, job, repository, schedule, ScheduleDefinition, get_dagster_logger


@job(description = 'Parsing, tranforming and loading to CH NH launches')
def hn_launches_job():
    
    transform_load_hn_launches(parse_hn_launches())
    
@schedule(job = hn_launches_job,
          cron_schedule = "@daily",
          execution_timezone = "Europe/Moscow",
          default_status = DefaultScheduleStatus.RUNNING)
def hn_launches_schedule(context):
    return {}
