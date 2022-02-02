#!/usr/bin/env python3
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ProcessPoolExecutor
from etl_brs import run_brs_pipeline
from etl_asr import run_asr_pipeline
from etl_frs import run_frs_pipeline
from etl_trs import run_trs_pipeline
import json

jobstores = {
    'mongo': MongoDBJobStore(),
}
executors = {
    'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}


if __name__ == "__main__":
    with open('dbconfig.json', 'r') as config_file:
            data = json.load(config_file)
            oltp_db = data['database']['oltp_dbname']
            ann_db = data['database']['ann_dbname']
            dwh_db = data['database']['dwh_dbname']
            client = data['database']['client']

    sched = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    sched.configure()
    @sched.scheduled_job('interval', hours=1)
    def run():
        current_t = datetime.utcnow()
        run_brs_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
        run_asr_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
        run_frs_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
        run_trs_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
    
    sched.start()
