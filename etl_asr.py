import pandas as pd
from datetime import datetime
from utils import *
import json


def word_cloud(df):
    """Extract individual words from the generated transcripts.

    Args:
        df (pandas.DataFrame): A dataframe containing raw ASR inference data

    Returns:
        pandas.DataFrame: A dataframe containing the same data but with duplicated rows for each 
        word in the transcript
    """
    df = df[['asrInfer', 'timestamp', 'channelName']]
    new_df = pd.DataFrame({})
    df['asrInfer_split'] = df['asrInfer'].apply(lambda x: x.split(' '))
    new_df = df.explode('asrInfer_split').reset_index(drop=True)
    for channel in pd.unique(new_df['channelName']):
        chl_df = new_df[new_df['channelName'] == channel].reset_index(drop=True)
        channel = channel + '_asr'
        yield chl_df, channel



def run_asr_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client='mongodb://localhost:27017'):
    """connects to MongoDB database, pulls the raw inference data, transforms it into word data
        and saves it in DWH. Then moves the raw data to annotator databases 
        and then deletes it from the OLTP databases

    Args:
        time_filter (datetime.object, optional): time_filter (datetime object, optional): Used to filter raw data on the basis of time.
         Defaults to None.
    """
    with open('dbconfig.json') as conf_file:
        settings = json.load(conf_file)
        tgt_coll_oltp = settings['speech']['tgt_coll_oltp']
        tgt_coll_ann = settings['speech']['tgt_coll_ann']

    asr_data, connector = pull_from_mongo(time_filter, client, oltp_db, tgt_coll_oltp)
    push_to_mongo(connector, asr_data, ann_db, tgt_coll_ann)
    delete_collection(connector, oltp_db, tgt_coll_oltp, curr_time=time_filter)
    # push_to_mongo(connector, asr_data, dwh_db, 'dwh_asr')
    asr_data.dropna(inplace=True)
    words = word_cloud(asr_data)
    for word, chnl in words:
        push_to_mongo(connector, word, dwh_db, chnl)


if __name__ == '__main__':
    with open('dbconfig.json', 'r') as config_file:
        data = json.load(config_file)
        oltp_db = data['database']['oltp_dbname']
        ann_db = data['database']['ann_dbname']
        dwh_db = data['database']['dwh_dbname']
        client = data['database']['client']

    current_t = datetime(2022, 1, 6, 12, 30)
    run_asr_pipeline(oltp_db, ann_db, dwh_db, time_filter=current_t, client=client)
    print(-1)
