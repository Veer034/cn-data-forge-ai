import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092,broker:29092"),
    'group_id':'vector_processor',
    'topic': os.getenv('KAFKA_TOPIC', 'messages')
}

# KAFKA_CONFIG = {
#     'bootstrap_servers':"localhost:9092",
#     'group_id':'vector_processor',
#     'topic': os.getenv('KAFKA_TOPIC', 'messages')
# }


ES_CONFIG = {
    'hosts': os.getenv('ES_HOSTS', 'http://localhost:9200'),
    'index_name': os.getenv('ES_INDEX_NAME', 'messages')
}