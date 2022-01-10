import pandas as pd
from utils import *


def word_cloud(df):
    df = df[['asrInfer', 'timestamp','channelName']]
    new_df = pd.DataFrame({})
    df['asrInfer_split'] = df['asrInfer'].apply(lambda x: x.split(' '))
    new_df = df.explode('asrInfer_split').reset_index(drop=True)
    return new_df


def run_asr_pipeline():
    asr_data, connector = connect_to_mongo(tgt_coll='asr')
    push_to_mongo(connector, asr_data, 'annotator_db', 'asr')
    delete_collection(connector, 'testdata', 'asr')
    push_to_mongo(connector, asr_data, 'data_warehouse', 'dwh_asr')
    asr_data.dropna(inplace=True)
    words = word_cloud(asr_data)
    push_to_mongo(connector, words, 'data_warehouse', 'asr_word_cloud')


if __name__ == '__main__':
    run_asr_pipeline()
    print(-1)
