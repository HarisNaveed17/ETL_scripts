from datetime import datetime
from utils import *


def run_frs_pipeline(time_filter=None):
    frs_data, connector = connect_to_mongo(time_filter, tgt_coll='frs')
    push_to_mongo(connector, frs_data, 'annotator_db', 'frs')
    delete_collection(connector, 'testdata', 'frs')
    push_to_mongo(connector, frs_data, 'data_warehouse', 'dwh_frs')
    frs_data.dropna()
    people, chnl = extend_inferdata(
        frs_data, infer_column='frsInfer', other_column=['timestamp', 'channelName'])
    for chnl_data in people:
        push_to_mongo(connector, chnl_data, 'data_warehouse', chnl)


if __name__ == '__main__':
    current_t = datetime(2022, 1, 5, 17, 45)
    run_frs_pipeline(current_t)
    print(-1)
