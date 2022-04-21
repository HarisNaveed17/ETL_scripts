from datetime import datetime
from utils import *
import json


def extend_inferdata_text(df, infer_column, other_column):
    if other_column is not None:
        other_column.append(infer_column)
        df = df[other_column]

    # df[infer_column] = df[infer_column].apply(lambda x: literal_eval(x)) turn this on if working with a csv on local machine
    df['textInfer'] = df[infer_column].apply(lambda x: combine_infer_text(x))
    df.drop(infer_column, axis=1, inplace=True)
    for channel in pd.unique(df['channelName']):
        chl_df = df[df['channelName'] == channel].reset_index(drop=True)
        new_df = chl_df.explode('textInfer').reset_index(drop=True)
        channel = channel + f'_trs'
        yield new_df, channel


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
    ticker_words = extend_inferdata_text(
        trs_data, infer_column='output', other_column=['timestamp', 'channelName'])
    for ticker, chnl in ticker_words:
        push_to_mongo(connector, ticker, dwh_db, chnl)


if __name__ == '__main__':
    with open('dbconfig.json', 'r') as config_file:
        data = json.load(config_file)
        oltp_db = data['database']['oltp_dbname']
        ann_db = data['database']['ann_dbname']
        dwh_db = data['database']['dwh_dbname']
        client = data['database']['client']
    current_t = datetime(2022, 1, 5, 17, 45)
    run_trs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client=client)
    print(-1)
