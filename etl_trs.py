from utils import *


def run_trs_pipeline():
    asr_data, connector = connect_to_mongo(tgt_coll='trs')
    push_to_mongo(connector, asr_data, 'annotator_db', 'trs')
    delete_collection(connector, 'testdata', 'trs')
    push_to_mongo(connector, asr_data, 'data_warehouse', 'dwh_trs')


if __name__ == '__main__':
    run_trs_pipeline()
    print(-1)
