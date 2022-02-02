from datetime import datetime
from utils import *
import pandas as pd
import json


def ad_dur(df):
    """Calculates the duration time of each ad for each channel in the raw data

    Args:
        df (pandas.DataFrame): A dataframe containing raw brand inference data

    Yields:
        pandas.DataFrame: ad duration time data for each channel
    """

    brand_dict = {}
    # df.drop('_id', axis=1, inplace=True)
    # df['brsStatus'] = 0
    df = df.rename({'timestamp': 'start_time'}, axis=1)
    brand_names = pd.unique(df['brsInfer'])
    for i, j in enumerate(brand_names):
        brand_dict[j] = i
    df['brand_mapping'] = df['brsInfer'].apply(
        lambda x: brand_dict[x])
    for channel in pd.unique(df['channelName']):
        channel_data = df[df['channelName'] == channel].reset_index(drop=True)
        checkpts = channel_data[channel_data['brand_mapping'].diff()
                                != 0].index
        proc_chnl_data = channel_data.iloc[checkpts]
        proc_chnl_data.drop('brand_mapping', axis=1, inplace=True)
        proc_chnl_data['end_time'] = proc_chnl_data['start_time'].shift(
            -1).fillna(channel_data.iloc[-1].start_time)
        channel = channel + '_brs'
        yield proc_chnl_data, channel


def run_brs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client='mongodb://localhost:27017'):
    """ connects to MongoDB database, pulls the raw inference data, transforms it into ad duration data
        and saves it in DWH. Then moves the raw data to annotator databases 
        and then deletes it from the OLTP databases

    Args:
        time_filter (datetime object, optional): Used to filter raw data on the basis of time.
         Defaults to None.
    """
    with open('dbconfig.json') as conf_file:
        settings = json.load(conf_file)
        tgt_coll_oltp = settings['brand']['tgt_coll_oltp']
        tgt_coll_ann = settings['brand']['tgt_coll_ann']

    brand_data, connector = pull_from_mongo(time_filter, client, oltp_db, tgt_coll_oltp)
    proc_brs_data = ad_dur(brand_data)
    push_to_mongo(connector, brand_data, ann_db, tgt_coll_ann)
    delete_collection(connector, oltp_db, tgt_coll_oltp, curr_time=time_filter)
    for chnl_data, chnl in proc_brs_data:
        push_to_mongo(connector, chnl_data, dwh_db, chnl)


if __name__ == '__main__':
    with open('dbconfig.json', 'r') as config_file:
        data = json.load(config_file)
        oltp_db = data['oltp_dbname']
        ann_db = data['ann_dbname']
        dwh_db = data['dwh_dbname']
        client = data['client']
    # current_t = datetime.datetime.utcnow()
    current_t = datetime(2022, 1, 5, 17, 45)
    run_brs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client=client)
    print(-1)
