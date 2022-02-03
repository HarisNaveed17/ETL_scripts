from datetime import datetime
from utils import *
import json


def run_frs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client='mongodb://localhost:27017'):
    with open('dbconfig.json') as conf_file:
        settings = json.load(conf_file)
        tgt_coll_oltp = settings['face']['tgt_coll_oltp']
        tgt_coll_ann = settings['face']['tgt_coll_ann']

    frs_data, connector = pull_from_mongo(time_filter, client, oltp_db, tgt_coll_oltp)
    push_to_mongo(connector, frs_data, ann_db, tgt_coll_ann)
    delete_collection(connector, oltp_db, tgt_coll_oltp, time_filter)
    # push_to_mongo(connector, frs_data, dwh_db, 'dwh_frs')
    frs_data.dropna()
    people = extend_inferdata(
        frs_data, infer_column='frsInfer', other_column=['timestamp', 'channelName'])
    for chnl_data, chnl in people:
        push_to_mongo(connector, chnl_data, dwh_db, chnl)


if __name__ == '__main__':
    with open('dbconfig.json', 'r') as config_file:
        data = json.load(config_file)
        oltp_db = data['database']['oltp_dbname']
        ann_db = data['database']['ann_dbname']
        dwh_db = data['database']['dwh_dbname']
        client = data['database']['client']
    current_t = datetime(2022, 1, 5, 17, 45)
    run_frs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client=client)
    print(-1)
