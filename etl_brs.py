import pandas as pd
# from datetime import datetime, timezone
from utils import *


def ad_dur(df):
    
    """Calculates the duration time of each ad for each channel in the raw data

    Args:
        df (pandas.DataFrame): A dataframe containing raw brand inference data

    Yields:
        pandas.DataFrame: ad duration time data for each channel
    """

    brand_dict = {}
    df.drop('_id', axis=1, inplace=True)
    df['brsStatus'] = 0
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
        yield proc_chnl_data


def run_brs_pipeline():
    brand_data, connector = connect_to_mongo()
    proc_brs_data = ad_dur(brand_data)
    push_to_mongo(connector, brand_data, 'annotator_db', 'brs')
    delete_collection(connector, 'testdata', 'brs')
    for chnl_data in proc_brs_data:
        push_to_mongo(connector, chnl_data, 'data_warehouse', 'dwh_brs')


if __name__ == '__main__':
    # current_t = datetime.datetime.utcnow()
    # current_t = datetime(2021, 1, 5, 18, 0, tzinfo=timezone.utc)
    # current_t = pd.Timestamp('2021-1-5 18:00:00.100').tz_localize(tz='UTC')
    run_brs_pipeline()
    print(-1)
    

