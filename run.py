#!/usr/bin/env python3

from datetime import datetime
from time import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ProcessPoolExecutor
from etl_brs import run_brs_pipeline
from etl_asr import run_asr_pipeline
from etl_frs import run_frs_pipeline
from etl_trs import run_trs_pipeline
import json

with open('sched_config.json', 'r') as sched_file:
    sched_params = json.load(sched_file)
    exe_procs = sched_params['executors']['process_executors']
    coalesce = sched_params['job_defaults']['coalesce']
    max_instances = sched_params['job_defaults']['max_instances']


jobstores = {
    'mongo': MongoDBJobStore(),
}
executors = {
    'processpool': ProcessPoolExecutor(exe_procs)
}
job_defaults = {
    'coalesce': coalesce,
    'max_instances': max_instances
}


if __name__ == "__main__":
    with open('dbconfig.json', 'r') as config_file:
            data = json.load(config_file)
            oltp_db = data['database']['oltp_dbname']
            ann_db = data['database']['ann_dbname']
            dwh_db = data['database']['dwh_dbname']
            client = data['database']['client']
            interval_length = data['database']['time_interval_seconds']
            time_filter = data['database']['time_filter']

    sched = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    sched.configure()
    @sched.scheduled_job('interval', seconds=interval_length)
    def run():
        if time_filter:
            current_t = datetime.utcnow()
        else:
            current_t = None
        run_brs_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
        run_asr_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
        run_frs_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
        run_trs_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
    
    sched.start()
    print(-1)