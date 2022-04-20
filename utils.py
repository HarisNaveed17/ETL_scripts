import pymongo
import pandas as pd


# CRUD (CREATE, READ, UPDATE, DELETE) OPERATION FUNCTIONS


def connect_to_mongo(client="mongodb://localhost:27017"):
    return pymongo.MongoClient(client)


def pull_from_mongo(curr_time=None,  client="mongodb://localhost:27017", tgt_db='testdata', tgt_coll='brs'):
    connection = connect_to_mongo(client)
    db = connection[tgt_db]
    table = db[tgt_coll]
    if curr_time is not None:
        db_data = pd.DataFrame(
            list(table.find({'timestamp': {"$lt": curr_time}})))
    else:
        db_data = pd.DataFrame(list(table.find({})))
    return db_data, connection


def push_to_mongo(connection, df, tgt_db, tgt_coll):
    dwh = connection[tgt_db]
    dwh[tgt_coll].insert_many(df.to_dict('records'))


def delete_collection(connection, tgt_db, tgt_coll, curr_time=None):
    db = connection[tgt_db]
    table = db[tgt_coll]
    if curr_time is not None:
        del_data = table.delete_many({'timestamp': {'$lt': curr_time}})
    else:
        del_data = table.delete_many({})

# TRANSFORMATION FUNCTIONS USED BY TWO OR MORE MODULES (ASR, BRS, FRS, TRS)


def combine_infer_text(ticker_list):
        infer_list = []
        for output in ticker_list:
            infer_list.append(output['infer'])
        return infer_list

# def extend_inferdata(df, infer_column, other_column=None, mode='frs'):
#     """Duplicates the raw data entries according to the number of unique inference outputs present in 
#     each entry.

#     Args:
#         df (pandas.DataFrame): raw inference data
#         other_column (list, optional): The list of columns to use from the raw data. Defaults to None.
#         infer_column (str): The name of the inference column in the raw data
#         mode (str): The name of the module whose data is being processed. Defaults to frs

#     Yields:
#         pandas.DataFrame: A dataframe containing the same data but with duplicated rows for each 
#         word in the transcript.
#     """
#     new_df = pd.DataFrame({})
#     if other_column is not None:
#         other_column.append(infer_column)
#         df = df[other_column]

#     # df[infer_column] = df[infer_column].apply(lambda x: literal_eval(x))
#     for channel in pd.unique(df['channelName']):
#         chl_df = df[df['channelName'] == channel].reset_index(drop=True)
#         if mode == 'frs':
#             new_df = chl_df.explode(infer_column).reset_index(drop=True)
#             channel = channel + f'_{mode}'

#         if mode == 'trs':
#             chl_df['Infer_split'] = chl_df[infer_column].apply(
#                 lambda x: ' '.join(x).split(' '))
#             new_df = chl_df.explode('Infer_split').reset_index(drop=True)
#             channel = channel + f'_{mode}'

#         yield new_df, channel



if __name__ == '__main__':
    print(-1)
