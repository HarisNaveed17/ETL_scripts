from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ProcessPoolExecutor
from pytz import utc
from etl_asr import run_asr_pipeline
from etl_brs import run_brs_pipeline
from etl_frs import run_frs_pipeline
from etl_trs import run_trs_pipeline
from datetime import datetime

# This is a lot simpler with the schedule library but I wanted to add handling for system restarts and power outages.

jobstores = {
    'mongo': MongoDBJobStore(),
}
executors = {
    'processpool': ProcessPoolExecutor(8)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}


sched = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)
@sched.scheduled_job('interval', hours=1)
def run():
    time_filter = datetime.utcnow() # If it doesn't filter try replacing utcnow() with now()
    run_brs_pipeline(time_filter)
    run_frs_pipeline(time_filter)
    run_asr_pipeline(time_filter)
    run_trs_pipeline(time_filter)

sched.start()

