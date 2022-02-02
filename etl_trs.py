from datetime import datetime
from utils import *
import json


def run_trs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client='mongodb://localhost:27017'):
    with open('dbconfig.json') as conf_file:
        settings = json.load(conf_file)
        tgt_coll_oltp = settings['ticker']['tgt_coll_oltp']
        tgt_coll_ann = settings['ticker']['tgt_coll_ann']

    trs_data, connector = pull_from_mongo(time_filter, client, oltp_db, tgt_coll_oltp)
    push_to_mongo(connector, trs_data, ann_db, tgt_coll_ann)
    delete_collection(connector, oltp_db, tgt_coll_oltp)
    # push_to_mongo(connector, trs_data, dwh_db, 'dwh_trs')
    trs_data.dropna(inplace=True)
    ticker_words = extend_inferdata(
        trs_data, infer_column='trsInfer', other_column=['timestamp', 'channelName'], mode='trs')
    for ticker, chnl in ticker_words:
        push_to_mongo(connector, ticker, dwh_db, chnl)


if __name__ == '__main__':
    with open('dbconfig.json', 'r') as config_file:
        data = json.load(config_file)
        oltp_db = data['oltp_dbname']
        ann_db = data['ann_dbname']
        dwh_db = data['dwh_dbname']
        client = data['client']
    current_t = datetime(2022, 1, 5, 17, 45)
    run_trs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client=client)
    print(-1)
