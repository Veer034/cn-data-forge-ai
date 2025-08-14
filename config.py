import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group_id':'vector_processor',
    'document_storage_request_topic': os.getenv('TENANT_DOCUMENTS_VECTOR_STORAGE_REQUEST_TOPIC', 'tenant.documents.vector.storage.request'),
    'document_storage_request_dlq_topic': os.getenv('TENANT_DOCUMENTS_VECTOR_STORAGE_REQUEST_DLQ_TOPIC', 'tenant.documents.vector.storage.request_DLQ'),
    'faq_storage_request_topic': os.getenv('TENANT_FAQS_VECTOR_STORAGE_REQUEST_TOPIC', 'tenant.faq.vector.storage.request'),
    'faq_storage_request_dlq_topic': os.getenv('TENANT_FAQS_VECTOR_STORAGE_REQUEST_DLQ_TOPIC', 'tenant.faq.vector.storage.request_DLQ'),
    'vector_storage_response_topic': os.getenv('DATA_FORGE_AI_VECTOR_STORAGE_RESPONSE_TOPIC', 'data.forge.ai.vector.storage.response')
}

ES_CONFIG = {
    'hosts': [
        f"https://{host.strip()}" for host in os.getenv('ES_HOSTS', 'localhost:9200').split(",")
    ],
    'username': os.getenv('ES_USERNAME',''),
    'password': os.getenv('ES_PASSWORD',''),
    'ca_certs': '/usr/local/share/ca-certificates/elasticsearch.crt',  # Point to certificate file
    'verify_certs': False,
    'ssl_show_warn': True,
    'tenant_document_index_name': os.getenv('ES_TENANT_DOCUMENTS_VECTOR_INDEX_NAME', 'tenant-documents-vector')
}