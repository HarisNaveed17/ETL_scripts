from datetime import datetime
from utils import *
import json


def extend_inferdata_face(df, infer_column_tag, other_column=None):
    """extends labels of detected faces from multiple columns to multiple rows against the same frame and
    timestamp (duplicates rows equal to number of labelled faces)

    Args:
        df (Pandas DataFrame): Input raw dataframe
        infer_column_tag (string): a string label contained in the name of every face label column name (frsInfer.O.Label
        in this case)
        other_column (list): list of other columns that the user wants to include. Defaults to None

    Yields:
        tuple: a tuple of the processed dataframe and channel name
    """
    
    lab_cols = [lab for lab in df.columns if infer_column_tag in lab]
    if other_column is not None:
        other_column.extend(lab_cols)
        ndf = df[other_column]
    ndf['combined'] = ndf[lab_cols].values.tolist()
    ndf.drop(lab_cols, axis=1, inplace=True)
    for channel in pd.unique(df['channelName']):
        chl_df = df[df['channelName'] == channel].reset_index(drop=True)
        new_df = chl_df.explode('combined').reset_index(drop=True)
        channel = channel + f'_frs'
        yield new_df, channel


def run_frs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client='mongodb://localhost:27017'):
    with open('dbconfig.json') as conf_file:
        settings = json.load(conf_file)
        tgt_coll_oltp = settings['face']['tgt_coll_oltp']
        tgt_coll_ann = settings['face']['tgt_coll_ann']

    frs_data, connector = pull_from_mongo(time_filter, client, oltp_db, tgt_coll_oltp)
    push_to_mongo(connector, frs_data, ann_db, tgt_coll_ann)
    delete_collection(connector, oltp_db, tgt_coll_oltp, time_filter)
    # push_to_mongo(connector, frs_data, dwh_db, 'dwh_frs')
    frs_data.dropna()
    people = extend_inferdata_face(
        frs_data, infer_column='frsInfer', other_column=['timestamp', 'channelName'])
    for chnl_data, chnl in people:
        push_to_mongo(connector, chnl_data, dwh_db, chnl)


if __name__ == '__main__':
    with open('dbconfig.json', 'r') as config_file:
        data = json.load(config_file)
        oltp_db = data['database']['oltp_dbname']
        ann_db = data['database']['ann_dbname']
        dwh_db = data['database']['dwh_dbname']
        client = data['database']['client']
    current_t = datetime(2022, 1, 5, 17, 45)
    run_frs_pipeline(oltp_db, ann_db, dwh_db, time_filter=None, client=client)
    print(-1)
