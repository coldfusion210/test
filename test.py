import os
import json
import logging
from elasticsearch import Elasticsearch

# Configure logger
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

# ElasticSearch configuration
ES_HOST = 'localhost'
ES_PORT = 9200
ES_INDEX_NAME = 'cdr'
ES_DOC_TYPE = 'call_record'
ES_USER = '<YOUR_USERNAME>'
ES_PASSWORD = '<YOUR_PASSWORD>'

# Function to parse a single CDR record and convert it to JSON
def parse_cdr_record(record):
    fields = record.strip().split(',')
    if len(fields) != 7:
        logging.warning(f'Invalid CDR record: {record}')
        return None
    a_msisdn, b_msisdn, call_date_time, call_duration, a_cell_id, call_start_time, call_end_time = fields
    return {
        'a_msisdn': a_msisdn,
        'b_msisdn': b_msisdn,
        'call_date_time': call_date_time,
        'call_duration': int(call_duration),
        'a_cell_id': a_cell_id,
        'call_start_time': call_start_time,
        'call_end_time': call_end_time
    }

# Function to process a CDR file and push its records to ElasticSearch
#TODO: Add code to handle large CDR files by splitting the file into multiple chunks
def process_cdr_file(file_path, es):
    file_name = os.path.basename(file_path)
    index_name = f'{ES_INDEX_NAME}_{file_name}'
    with open(file_path) as f:
        for line in f:
            record = parse_cdr_record(line)
            if record is not None:
                es.index(index=index_name, doc_type=ES_DOC_TYPE, body=record)

# Connect to ElasticSearch
es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}], http_auth=(ES_USER, ES_PASSWORD))

# Process all CDR files in a directory
cdr_dir = '/path/to/cdr/files'
for file_name in os.listdir(cdr_dir):
    file_path = os.path.join(cdr_dir, file_name)
    if os.path.isfile(file_path) and file_name.endswith('.cdr'):
        logging.info(f'Processing CDR file: {file_name}')
        process_cdr_file(file_path, es)
        logging.info(f'Finished processing CDR file: {file_name}')
