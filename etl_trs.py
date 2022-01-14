from datetime import datetime
from utils import *


def run_trs_pipeline(time_filter=None):
    trs_data, connector = connect_to_mongo(time_filter, tgt_coll='trs')
    push_to_mongo(connector, trs_data, 'annotator_db', 'trs')
    delete_collection(connector, 'testdata', 'trs')
    push_to_mongo(connector, trs_data, 'data_warehouse', 'dwh_trs')
    trs_data.dropna(inplace=True)
    ticker_words, chnl = extend_inferdata(
        trs_data, infer_column='trsInfer', other_cols=['timestamp', 'channelName'])
    for ticker in ticker_words:
        push_to_mongo(connector, ticker, 'data_warehouse', chnl)


if __name__ == '__main__':
    current_t = datetime(2022, 1, 5, 17, 45)
    run_trs_pipeline(current_t)
    print(-1)
