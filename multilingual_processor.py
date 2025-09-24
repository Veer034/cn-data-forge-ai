import re
import json
import logging
import asyncio
import datetime
import uuid
import signal
import sys
import httpx
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from sentence_transformers import SentenceTransformer
from elasticsearch import AsyncElasticsearch
from confluent_kafka import Consumer, Producer, KafkaError
from pydantic import BaseModel
from contextvars import ContextVar

# Import our utility modules
from document_section_utils import DocumentSectionUtils
from chunking_utils import ImprovedChunkingUtils
from config import KAFKA_CONFIG, ES_CONFIG, MISTRAL_CONFIG
from libaryLanguage import LibraryLanguageDetector
from logger_config import get_logger

logger = get_logger(__name__)
tracking_id_var = ContextVar("X-Tracking-ID", default="NA")

class DataStorageDto(BaseModel):
    tenantId: str
    storedIds: List[str] 
    isDone: bool
    dataType: str

class MultilingualMessageProcessor:
    """Multilingual processor with improved chunking but original schema"""
    
    def __init__(self, es_config: Dict[str, Any], kafka_config: Dict[str, Any], model_path: Optional[str] = None):
        """Initialize the processor"""
        
        logger.info("=" * 60)
        logger.info("INITIALIZING ENHANCED MULTILINGUAL MESSAGE PROCESSOR")
        logger.info("=" * 60)

        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        logger.info("✓ Signal handlers configured for graceful shutdown")
        
        self.es_config = es_config
        self.kafka_config = kafka_config
        self.http_client = httpx.AsyncClient(timeout=300.0)
        
        # Initialize SentenceTransformer
        logger.info("Initializing SentenceTransformer model...")
        model_name = 'paraphrase-multilingual-mpnet-base-v2'
        try:
            if model_path:
                self.st_model = SentenceTransformer(model_path)
                logger.info("✓ Model loaded from local path")
            else:
                self.st_model = SentenceTransformer(model_name)
                logger.info("✓ Model loaded from Hugging Face")
        except Exception as e:
            logger.error(f"✗ CRITICAL: Failed to load model: {e}")
            raise

        # Initialize Elasticsearch
        logger.info("Initializing Elasticsearch...")
        try:
            self.es_client = AsyncElasticsearch(
                es_config['hosts'],
                basic_auth=(es_config['username'], es_config['password']),
                verify_certs=es_config.get('verify_certs', True),
                ssl_show_warn=es_config.get('ssl_show_warn', True),
                ca_certs=es_config.get('ca_certs'),
                retry_on_timeout=True,
                max_retries=3
            )
            logger.info("✓ Elasticsearch client initialized")
        except Exception as e:
            logger.error(f"✗ CRITICAL: Elasticsearch init failed: {e}")
            raise
        
        # Kafka configuration
        self.consumer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers'],
            'group.id': kafka_config['group_id'],
            'auto.offset.reset': kafka_config.get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 15000,
            'request.timeout.ms': 65000
        }
        
        self.producer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers']
        }
        
        # Kafka topics
        self.document_request_topic = kafka_config['document_storage_request_topic']
        self.document_dlq_topic = kafka_config['document_storage_request_dlq_topic']
        self.faq_request_topic = kafka_config['faq_storage_request_topic']
        self.faq_dlq_topic = kafka_config['faq_storage_request_dlq_topic']
        self.response_topic = kafka_config['vector_storage_response_topic']

        self.producer = None

        # Initialize language detector
        try:
            self.libraryDetector = LibraryLanguageDetector()
            logger.info("✓ Language detector initialized")
        except Exception as e:
            logger.error(f"✗ Language detector failed: {e}")

    def _signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {sig}, shutting down...")
        self.shutdown_requested = True

    def detect_best_language(self, text):
        """Detect language for text"""
        if len(self.libraryDetector.available_libraries) > 0:
            lib_signals = self.libraryDetector._detect_with_libraries(text)
            if lib_signals:
                best_signal = max(lib_signals, key=lambda x: x[2])
                _, lang, conf = best_signal
                if conf > 0.5:
                    return lang
        return 'en'

    async def extract_keywords_and_context_with_mistral(self, text: str, section_title: str = "", 
                                                        language: str = 'en', max_keywords: int = 20) -> Dict[str, Any]:
        """Extract keywords using Mistral - simplified version"""
        try:
            system_prompt = f"""Extract {max_keywords} keywords and create a context summary. Return ONLY JSON:
{{
    "keywords": ["word1", "phrase1", "concept1"],
    "context_summary": "Brief summary of the content"
}}

Rules:
- Same language as input text
- Important nouns, technical terms, key phrases
- No stop words
- Focus on searchable terms"""

            section_context = f"Section: {section_title}\n\n" if section_title and section_title != "General" else ""
            user_prompt = f"{section_context}Text: {text[:1200]}"

            data = {
                "model": MISTRAL_CONFIG['model'],
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "stream": False,
                "max_tokens": 250,
                "temperature": 0.1
            }

            start_time = datetime.datetime.now()


            response = await self.http_client.post(
                MISTRAL_CONFIG['chat_url'],
                headers={"Content-Type": "application/json"},
                json=data,
                timeout=MISTRAL_CONFIG['timeout']
            )
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Mistral API response received in {duration:.3f} seconds")
            
            if response.status_code != 200:
                return self._fallback_extraction(text, section_title, max_keywords)

            response_data = response.json()
            
            if 'message' in response_data and 'content' in response_data['message']:
                content = response_data['message']['content'].strip()
                
                try:
                    extraction_data = json.loads(content)
                    return self._process_mistral_extraction(extraction_data, text, max_keywords)
                except json.JSONDecodeError as ex:
                    logger.error(f"Json decode error: {str(ex)}")
                    return self._fallback_extraction(text, section_title, max_keywords)
            
            return self._fallback_extraction(text, section_title, max_keywords)
            
        except Exception as e:
            logger.error(f"Mistral extraction error: {str(e)}")
            return self._fallback_extraction(text, section_title, max_keywords)
    
    def _process_mistral_extraction(self, extraction_data: Dict, text: str, max_keywords: int) -> Dict[str, Any]:
        """Process Mistral response - simplified"""
        processed = {"keywords": [], "context_summary": ""}
        
        # Extract keywords
        if 'keywords' in extraction_data and isinstance(extraction_data['keywords'], list):
            keywords = []
            for keyword in extraction_data['keywords'][:max_keywords]:
                if isinstance(keyword, str) and len(keyword.strip()) > 1:
                    keywords.append(keyword.strip())
            processed["keywords"] = keywords
        
        # Extract summary
        if 'context_summary' in extraction_data and isinstance(extraction_data['context_summary'], str):
            summary = extraction_data['context_summary'].strip()
            if len(summary) > 10:
                processed["context_summary"] = summary
        
        return processed

    def _fallback_extraction(self, text: str, section_title: str, max_keywords: int) -> Dict[str, Any]:
        """Simple fallback extraction"""
        words = text.lower().split()
        word_freq = {}
        
        for word in words:
            word = word.strip('.,;:!?()[]{}')
            if len(word) > 3 and word not in ['the', 'and', 'or', 'but', 'with', 'from', 'this', 'that']:
                word_freq[word] = word_freq.get(word, 0) + 1
        
        keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:max_keywords]
        keyword_list = [word for word, freq in keywords]
        
        # Simple summary
        first_sentence = text.split('.')[0][:100] if '.' in text else text[:100]
        summary = f"Content from {section_title}: {first_sentence}..." if section_title else first_sentence
        
        return {
            "keywords": keyword_list,
            "context_summary": summary
        }

    async def process_document_message_simplified(self, message: Dict[str, Any]):
        """Process document with improved chunking but original schema"""
        
        content = message.get('content')
        if not content:
            logger.warning("Message has no content")
            return

        tenant_id = message.get('tenantId')
        metadata = message.get('metadata', {}) or {}
        document_id = message.get('documentId')
        
        # Detect language
        language = metadata.get('language')
        if not language:
            language = self.detect_best_language(content)
            metadata['language'] = language

        # Use improved chunking
        sections = DocumentSectionUtils.identify_document_sections(content, language)
        chunks = ImprovedChunkingUtils.extract_chunks_with_section_analysis(content, language, sections)
        
        logger.info(f"Created {len(chunks)} optimized chunks)")
        
        # Process each chunk with ORIGINAL schema
        indexing_tasks = []
        for i, chunk in enumerate(chunks):
            # Generate vector embedding
            vector = self.st_model.encode(chunk.text).tolist()
            
            # Extract simple keywords
            extracted_keywords = await self.extract_keywords_and_context_with_mistral(
                chunk.text, chunk.section, language, max_keywords=15
            )

            
            keywords = extracted_keywords.get('keywords', [])
            context_summary = extracted_keywords.get('context_summary', '')
            
            # Prepare metadata (keep original structure)
            combined_metadata = {**metadata}
            combined_metadata.update(chunk.metadata.model_dump())
            combined_metadata['chunkIndex'] = i
            combined_metadata['totalChunks'] = len(chunks)
            combined_metadata['hasOverlap'] = i > 0
            combined_metadata['language'] = language
            
            # Use ORIGINAL schema - no extra fields
            chunk_doc = {
                'content': chunk.text,
                'contentVector': vector,
                'tenantId': tenant_id,
                'documentId': document_id,
                'chunkType': chunk.type,
                'sectionTitle': chunk.section,
                'metadata': combined_metadata,
                
                # Keep only original fields
                'chunkPosition': i,
                'totalChunks': len(chunks),
                'hasOverlap': i > 0,
                'contextSummary': context_summary,
                
                # Simple keywords (keeping original field names)
                'keywords': keywords,
                'keywordsText': ' '.join(keywords),
                'language': language,
                'keywordCount': len(keywords)
            }

            chunk_id = f"{document_id}_chunk_{i}"
            logger.info(f"Indexing chunk {i+1}/{len(chunks)}: {chunk.type} ({len(chunk.text)} chars)")

            indexing_tasks.append(
                self.es_client.index(
                    index=self.es_config['tenant_document_index_name'],
                    document=chunk_doc,
                    id=chunk_id
                )
            )
        
        # Index all chunks
        await asyncio.gather(*indexing_tasks)
        
        # Send response
        await self.publish_kafka_message(
            self.response_topic,
            tenant_id,
            DataStorageDto(
                tenantId=tenant_id,
                storedIds=[document_id],
                isDone=True,
                dataType='doc'
            )
        )

        logger.info(f"✓ Successfully indexed {len(chunks)} consolidated chunks for document {document_id}")

    async def process_faq_message(self, message: Dict[str, Any]):
        """Process FAQ message - unchanged"""
        tenant_id = message.get('tenantId')
        metadata = message.get('metadata', {}) or {}
        content_records = message.get('content', []) or []
        
        processed_faq_ids = set()
        
        for content in content_records:
            faq_id = content.get('faqId')
            if not faq_id:
                logger.warning(f"Skipping record with missing faqId for tenant: {tenant_id}")
                continue
                
            processed_faq_ids.add(faq_id)
            record_metadata = metadata.copy()
            record_metadata["faqId"] = faq_id
            
            question = content.get("question", "")
            answer = content.get("answer", "")
            link = content.get("link", "")
            paraphrases = content.get('paraphrases', []) or []

            language = record_metadata.get('language')
            if not language:
                language = self.detect_best_language(question + " " + answer)
                record_metadata['language'] = language
            
            # Process main Q&A
            document_id = str(uuid.uuid4())
            store = f"Question: {question} \n Answer: {answer} \n Link: {link}"
            vector = self.st_model.encode(store).tolist()
            
            document = {
                'content': store,
                'contentVector': vector,
                'tenantId': tenant_id,
                'documentId': document_id,
                'chunkType': "faq",
                'sectionTitle': "faq",
                'metadata': record_metadata
            }
                
            await self.es_client.index(
                index=self.es_config['tenant_document_index_name'],
                document=document,
                id=document_id
            )
            
            # Process paraphrases
            for paraphrase in paraphrases:
                paraphrase_id = str(uuid.uuid4())
                store = f"Question: {paraphrase} \n Answer: {answer} \n Link: {link}"
                vector = self.st_model.encode(store).tolist()
                
                document = {
                    'content': store,
                    'contentVector': vector,
                    'tenantId': tenant_id,
                    'documentId': paraphrase_id,
                    'chunkType': "faq",
                    'sectionTitle': "faq",
                    'metadata': record_metadata
                }
                    
                await self.es_client.index(
                    index=self.es_config['tenant_document_index_name'],
                    document=document,
                    id=paraphrase_id
                )
        
        # Cleanup and response
        delete_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"tenantId": tenant_id}},
                        {"term": {"metadata.inactive": True}}
                    ]
                }
            }
        }
        
        await self.es_client.delete_by_query(
            index=self.es_config['tenant_document_index_name'], 
            body=delete_query
        )
        
        await self.publish_kafka_message(
            self.response_topic,
            tenant_id,
            DataStorageDto(
                tenantId=tenant_id,
                storedIds=list(processed_faq_ids),
                isDone=True,
                dataType='faq'
            )
        )
            
        logger.info(f"Successfully indexed {len(processed_faq_ids)} FAQs for tenant: {tenant_id}")

    async def publish_kafka_message(self, topic: str, key: str, message: Any):
        """Publish Kafka message"""
        serialized_key = str(key).encode("utf-8")
        
        if isinstance(message, BaseModel):
            serialized_value = json.dumps(message.model_dump()).encode("utf-8")
        else:
            serialized_value = json.dumps(message).encode("utf-8")
        
        future = asyncio.Future()
        
        def delivery_callback(err, msg):
            if err:
                future.set_exception(Exception(f"Message delivery failed: {err}"))
            else:
                future.set_result(msg)
        
        tracking_id = tracking_id_var.get() or "NA"
        tracking_id_str = str(tracking_id)

        self.producer.produce(
            topic,
            key=serialized_key,
            value=serialized_value,
            callback=delivery_callback,
            headers=[("X-Tracking-ID", tracking_id_str)]
        )
        self.producer.poll(1)
        self.producer.flush()
        
        return await future

    async def _setup_elasticsearch_indices(self):
        """Setup Elasticsearch with ORIGINAL schema"""
        
        chunks_index = self.es_config['tenant_document_index_name']
        exists = await self.es_client.indices.exists(index=chunks_index)
        
        if not exists:
            await self.es_client.indices.create(
                index=chunks_index,
                settings={
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "keyword_analyzer": {
                                "type": "custom",
                                "tokenizer": "keyword",
                                "filter": ["lowercase", "trim"]
                            }
                        }
                    }
                },
                mappings={
                    "properties": {
                        # ORIGINAL schema - no changes
                        "content": {"type": "text"},
                        "contentVector": {
                            "type": "dense_vector",
                            "dims": 768,
                            "index": True,
                            "similarity": "cosine"
                        },
                        "tenantId": {"type": "keyword"},
                        "documentId": {"type": "keyword"},
                        "chunkType": {"type": "keyword"},
                        "sectionTitle": {"type": "text"},
                        "metadata": {"type": "object", "enabled": True},
                        "chunkPosition": {"type": "integer"},
                        "totalChunks": {"type": "integer"},
                        "hasOverlap": {"type": "boolean"},
                        "contextSummary": {"type": "text"},
                        "keywords": {"type": "keyword", "ignore_above": 100},
                        "keywordsText": {"type": "text", "analyzer": "keyword_analyzer"},
                        "language": {"type": "keyword"},
                        "keywordCount": {"type": "integer"}
                    }
                }
            )
            logger.info(f"Created chunks index with original schema: {chunks_index}")

    async def shutdown(self):
        """Graceful shutdown of resources"""
        logger.info("=" * 40)
        logger.info("INITIATING GRACEFUL SHUTDOWN")
        logger.info("=" * 40)
        
        try:
            if self.es_client:
                logger.info("Closing Elasticsearch connection...")
                await self.es_client.close()
                logger.info("✓ Elasticsearch connection closed")
                
            if self.producer:
                logger.info("Flushing Kafka producer...")
                self.producer.flush()
                logger.info("✓ Kafka producer flushed")
            
            logger.info("=" * 40)
            logger.info("✓ GRACEFUL SHUTDOWN COMPLETED")
            logger.info("=" * 40)
            
        except Exception as e:
            logger.error(f"✗ Error during shutdown: {e}")