from datetime import datetime
from utils import *


def run_frs_pipeline(time_filter=None):
    asr_data, connector = connect_to_mongo(time_filter, tgt_coll='frs')
    push_to_mongo(connector, asr_data, 'annotator_db', 'frs')
    delete_collection(connector, 'testdata', 'frs')
    push_to_mongo(connector, asr_data, 'data_warehouse', 'dwh_frs')


if __name__ == '__main__':
    current_t = datetime(2022, 1, 5, 17, 45)
    run_frs_pipeline(current_t)
    print(-1)
