from ops.parse_load_growjo import parse_load_growjo
from dagster import op, Out, In, graph, job, repository, schedule, ScheduleDefinition, get_dagster_logger


@job()
def growjo_job():
    
    parse_load_growjo()
    
basic_schedule = ScheduleDefinition(job = growjo_job, cron_schedule = '@daily')