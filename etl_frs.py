from utils import *


def run_frs_pipeline():
    asr_data, connector = connect_to_mongo(tgt_coll='frs')
    push_to_mongo(connector, asr_data, 'annotator_db', 'frs')
    delete_collection(connector, 'testdata', 'frs')
    push_to_mongo(connector, asr_data, 'data_warehouse', 'dwh_frs')


if __name__ == '__main__':
    run_frs_pipeline()
    print(-1)
