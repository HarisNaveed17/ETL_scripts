import pymongo
import pandas as pd


def connect_to_mongo(curr_time=None,  client="mongodb://localhost:27017", tgt_db='testdata', tgt_coll='brs'):
    connection = pymongo.MongoClient(client)
    db = connection[tgt_db]
    table = db[tgt_coll]
    db_data = pd.DataFrame(list(table.find({})))
    return db_data, connection


def push_to_mongo(connection, df, tgt_db, tgt_coll):
    dwh = connection[tgt_db]
    dwh[tgt_coll].insert_many(df.to_dict('records'))


def delete_collection(connection, tgt_db, tgt_coll):
    db = connection[tgt_db]
    table = db[tgt_coll]
    del_data = table.delete_many({})