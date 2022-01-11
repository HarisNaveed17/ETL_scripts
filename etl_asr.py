import pandas as pd
from datetime import datetime
from utils import *


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
    return new_df


def run_asr_pipeline(time_filter=None):
    """connects to MongoDB database, pulls the raw inference data, transforms it into word data
        and saves it in DWH. Then moves the raw data to annotator databases 
        and then deletes it from the OLTP databases

    Args:
        time_filter (datetime.object, optional): time_filter (datetime object, optional): Used to filter raw data on the basis of time.
         Defaults to None.
    """
    asr_data, connector = connect_to_mongo(time_filter, tgt_coll='asr')
    push_to_mongo(connector, asr_data, 'annotator_db', 'asr')
    delete_collection(connector, 'testdata', 'asr')
    push_to_mongo(connector, asr_data, 'data_warehouse', 'dwh_asr')
    asr_data.dropna(inplace=True)
    words = word_cloud(asr_data)
    push_to_mongo(connector, words, 'data_warehouse', 'asr_word_cloud')


if __name__ == '__main__':
    current_t = datetime(2022, 1, 5, 17, 45)
    run_asr_pipeline(current_t)
    print(-1)
