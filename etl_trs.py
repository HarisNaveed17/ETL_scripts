from utils import *
from datetime import datetime

def run_trs_pipeline(time_filter=None):
    asr_data, connector = connect_to_mongo(time_filter, tgt_coll='trs')
    push_to_mongo(connector, asr_data, 'annotator_db', 'trs')
    delete_collection(connector, 'testdata', 'trs')
    push_to_mongo(connector, asr_data, 'data_warehouse', 'dwh_trs')


if __name__ == '__main__':
    current_t = datetime(2022, 1, 5, 17, 45)
    run_trs_pipeline(current_t)
    print(-1)
