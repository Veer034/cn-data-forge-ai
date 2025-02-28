import datetime
import json
import os
import re
import uuid
import nltk
from sentence_transformers import SentenceTransformer
from elasticsearch import AsyncElasticsearch
import logging
import asyncio
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import httpx
from config import KAFKA_CONFIG, ES_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def publish_document_to_kafka(producer, topic, content, tenant_id, metadata=None):
    """
    Publishes a document to a Kafka topic, preserving existing metadata.
    
    Args:
        producer: KafkaProducer instance
        topic: Kafka topic to publish to
        content: Document content
        tenant_id: Tenant identifier
        metadata: Optional existing metadata dictionary
    
    Returns:
        Future for the message delivery
    """
    if metadata is None:
        metadata = {}
    
    # Create base message
    message = {
        'tenant_id': tenant_id,
        'content': content,
        'metadata': metadata.copy()  # Use a copy to avoid modifying the original
    }
    
    # Add timestamp if not present
    if 'timestamp' not in message['metadata']:
        message['metadata']['timestamp'] = datetime.datetime.now().isoformat()
    
    # Only detect document_type if not already provided
    if 'document_type' not in message['metadata']:
        message['metadata']['document_type'] = detect_document_type(content)
    
    # Only detect language if not already provided
    if 'language' not in message['metadata']:
        message['metadata']['language'] = detect_language(content)
    
    # Serialize and publish
    serialized_message = json.dumps(message).encode('utf-8')
    
    # Create an asyncio Future to wait for delivery report
    future = asyncio.Future()
    
    def delivery_callback(err, msg):
        if err:
            future.set_exception(Exception(f"Message delivery failed: {err}"))
        else:
            future.set_result(msg)
    
    producer.produce(topic, serialized_message, callback=delivery_callback)
    producer.poll(0)  # Trigger delivery callbacks
    
    return await future

def detect_document_type(content):
    """
    Attempts to detect the document type based on content patterns.
    Works with multiple languages by focusing on structural elements.
    """
    # Look for FAQ patterns in multiple languages
    faq_patterns = [
        r'FAQ|Frequently Asked Questions',  # English
        r'Preguntas Frecuentes|FAQ',        # Spanish
        r'FAQs|Foire Aux Questions',        # French
        r'常见问题|FAQ',                     # Chinese
        r'よくある質問|FAQ',                  # Japanese
        r'Häufig gestellte Fragen|FAQ'      # German
    ]
    
    if any(re.search(pattern, content, re.IGNORECASE) for pattern in faq_patterns):
        return 'faq'
    
    # Document type detection based on structure rather than language
    # Count question marks (works in most languages)
    question_count = len(re.findall(r'\?|？|¿', content))
    
    if question_count > 5:
        return 'qa_content'
    
    # Look for policy indicators (structural elements common in policies)
    policy_patterns = [
        r'Policy|Terms|Conditions',         # English
        r'Política|Términos|Condiciones',   # Spanish
        r'Politique|Conditions|Termes',     # French
        r'政策|条款|条件',                   # Chinese
        r'ポリシー|規約|条件',                # Japanese
        r'Richtlinie|Bedingungen'           # German
    ]
    
    if any(re.search(pattern, content, re.IGNORECASE) for pattern in policy_patterns):
        return 'policy'
    
    # Look for article/blog indicators
    article_patterns = [
        r'Article|Blog|Post',               # English
        r'Artículo|Blog|Entrada',           # Spanish
        r'Article|Blog|Publication',        # French
        r'文章|博客|帖子',                   # Chinese
        r'記事|ブログ|投稿',                  # Japanese
        r'Artikel|Blog|Beitrag'             # German
    ]
    
    if any(re.search(pattern, content, re.IGNORECASE) for pattern in article_patterns):
        return 'article'
    
    # Default type
    return 'general'

def detect_language(content):
    """
    Language detection that works with multiple languages.
    Falls back to basic detection if langdetect is not available.
    """
    try:
        from langdetect import detect
        return detect(content)
    except (ImportError, Exception) as e:
        logger.warning(f"Error using langdetect: {str(e)}. Falling back to basic detection.")
        
        # More sophisticated fallback than just checking for English
        # Note: This is a simplified approach - production systems should use a proper language detection library
        
        # Check for character sets that are distinctive to certain languages
        # Chinese/Japanese/Korean characters
        if re.search(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]', content):
            # Distinguish between Chinese, Japanese and Korean
            if re.search(r'[\u3040-\u309f\u30a0-\u30ff]', content):
                return 'ja'  # Japanese
            elif re.search(r'[\uac00-\ud7af]', content):
                return 'ko'  # Korean
            else:
                return 'zh'  # Chinese
        
        # Cyrillic (Russian, etc.)
        elif re.search(r'[\u0400-\u04FF]', content):
            return 'ru'  # Russian as default for Cyrillic
        
        # Arabic
        elif re.search(r'[\u0600-\u06FF]', content):
            return 'ar'
        
        # Greek
        elif re.search(r'[\u0370-\u03FF]', content):
            return 'el'
        
        # Latin-based languages - check for distinctive characters
        elif re.search(r'[áàâäãåāăąèéêëēėęíìîïīįıóòôöõøōőúùûüūųýÿźžż]', content):
            # This is simplified - would need more sophisticated rules to distinguish between 
            # Spanish, French, German, etc.
            return 'latin-script'
        
        # Default to English for primarily ASCII text
        else:
            return 'en'

class MultilingualMessageProcessor:
    
    def __init__(self, models_path=None):
        # Initialize SentenceTransformer with multilingual model
        model_name = 'paraphrase-multilingual-mpnet-base-v2'
        model_path = models_path or os.path.join(os.getcwd(), 'models', 'sentence_transformer')
        
        # Use downloaded model if available, otherwise use the model name directly
        if os.path.exists(model_path):
            logger.info(f"Loading model from local path: {model_path}")
            self.st_model = SentenceTransformer(model_path)
        else:
            logger.info(f"Local model not found. Loading model {model_name} from Hugging Face")
            self.st_model = SentenceTransformer(model_name)
        
        # Initialize async Elasticsearch client
        self.es_client = AsyncElasticsearch(
            ES_CONFIG['hosts'],
            basic_auth=(ES_CONFIG.get('username', ''), ES_CONFIG.get('password', '')),
            retry_on_timeout=True,
            max_retries=3
        )
        
        # Initialize httpx client
        self.http_client = httpx.AsyncClient()
        
        # Kafka configuration
        self.consumer_config = {
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['group_id'],
            'auto.offset.reset': KAFKA_CONFIG.get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 15000,
            'request.timeout.ms': 65000
        }
        
        self.producer_config = {
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers']
        }
        
        # Kafka topic
        self.topic = KAFKA_CONFIG['topic']
        
        # Try to download nltk data for multiple languages
        try:
            nltk.download('punkt', quiet=True)
        except Exception as e:
            logger.warning(f"Failed to download NLTK punkt: {str(e)}")

    async def _setup_elasticsearch_indices(self, base_index_name):
        """Setup Elasticsearch indices with proper mappings for vector search"""
        # Original documents index
        original_index = f"{base_index_name}_originals"
        exists = await self.es_client.indices.exists(index=original_index)
        if not exists:
            await self.es_client.indices.create(
                index=original_index,
                mappings={
                    "properties": {
                        "content": {"type": "text"},
                        "tenant_id": {"type": "keyword"},
                        "document_id": {"type": "keyword"},
                        "metadata": {"type": "object", "enabled": True}
                    }
                }
            )
            logger.info(f"Created originals index: {original_index}")
        
        # Chunks index with vector field
        chunks_index = base_index_name
        exists = await self.es_client.indices.exists(index=chunks_index)
        if not exists:
            await self.es_client.indices.create(
                index=chunks_index,
                mappings={
                    "properties": {
                        "content": {"type": "text"},
                        "content_vector": {
                            "type": "dense_vector",
                            "dims": 768,  # mpnet-base-v2 has 768 dimensions
                            "index": True,
                            "similarity": "cosine"
                        },
                        "tenant_id": {"type": "keyword"},
                        "document_id": {"type": "keyword"},
                        "chunk_type": {"type": "keyword"},
                        "section_title": {"type": "text"},
                        "metadata": {"type": "object", "enabled": True}
                    }
                }
            )
            logger.info(f"Created chunks index: {chunks_index}")

    def _extract_semantic_chunks(self, text, language=None, max_chunk_size=300):
        """
        Extract semantic chunks from text, preserving context.
        Works with multiple languages by using language-aware patterns and processing.
        
        Args:
            text: Document text to process
            language: Optional language code (e.g., 'en', 'es', 'fr')
            max_chunk_size: Maximum words per chunk
            
        Returns:
            List of chunk dictionaries
        """
        chunks = []
        
        # If language not provided, detect it
        if not language:
            language = detect_language(text)
        
        # First, try to identify document sections
        sections = self._identify_sections(text, language)
        
        # If no sections found, treat the whole document as one section
        if not sections:
            sections = [{"title": "General", "content": text}]
        
        for section in sections:
            section_title = section["title"]
            section_content = section["content"]
            
            # Extract explicit Q&A pairs (using language-aware patterns)
            explicit_qa_pairs = self._identify_qa_pairs(section_content, language)
            
            # Process explicit Q&A pairs
            if explicit_qa_pairs:
                for qa in explicit_qa_pairs:
                    chunks.append({
                        "text": qa["combined"],
                        "section": section_title,
                        "type": "qa_pair",
                        "metadata": {
                            "question": qa["question"],
                            "has_question": True,
                            "qa_format": "explicit"
                        }
                    })
                
                # Remove processed Q&A content from section
                for qa in explicit_qa_pairs:
                    section_content = section_content.replace(qa["combined"], "")
                    section_content = section_content.replace(f"Q: {qa['question']}\nA: {qa['answer']}", "")
                    # Handle other formats depending on language
                    if language == 'es':
                        section_content = section_content.replace(f"P: {qa['question']}\nR: {qa['answer']}", "")
                    elif language == 'fr':
                        section_content = section_content.replace(f"Q: {qa['question']}\nR: {qa['answer']}", "")
                    elif language == 'de':
                        section_content = section_content.replace(f"F: {qa['question']}\nA: {qa['answer']}", "")
            
            # Process remaining content to identify implicit Q&A and regular paragraphs
            if section_content.strip():
                # Split content into paragraphs
                paragraphs = [p for p in re.split(r'\n\s*\n', section_content) if p.strip()]
                
                for paragraph in paragraphs:
                    # Use NLTK for sentence tokenization (works for many languages)
                    try:
                        sentences = nltk.sent_tokenize(paragraph, language if language in ['en', 'es', 'fr', 'de', 'it', 'nl', 'pt'] else 'en')
                    except Exception:
                        # Fallback to simple tokenization if NLTK fails
                        sentences = re.split(r'(?<=[.!?])\s+', paragraph)
                    
                    # Identify potential questions (sentences ending with '?', '？', etc.)
                    question_indices = [i for i, s in enumerate(sentences) 
                                      if re.search(r'[?？¿]+\s*$', s.strip())]
                    
                    # Process implicit Q&A pairs
                    processed_indices = set()
                    if question_indices:
                        for idx in question_indices:
                            question = sentences[idx].strip()
                            
                            # Skip if already processed
                            if idx in processed_indices:
                                continue
                                
                            # Determine the answer - either next sentence or remaining sentences
                            if idx + 1 < len(sentences):
                                # Simple answer (next sentence)
                                if idx + 2 >= len(sentences) or idx + 2 in question_indices:
                                    answer = sentences[idx + 1].strip()
                                    answer_end_idx = idx + 1
                                else:
                                    # Take multiple sentences as the answer (up to next question or 3 sentences)
                                    answer_end_idx = min(idx + 4, len(sentences))
                                    next_question_idx = next((i for i in range(idx + 1, answer_end_idx) 
                                                            if i in question_indices), answer_end_idx)
                                    answer_end_idx = min(answer_end_idx, next_question_idx)
                                    answer = ' '.join(sentences[idx + 1:answer_end_idx]).strip()
                                
                                # Create an implicit Q&A chunk
                                if answer:  # Only if we have an answer
                                    # Format Q&A based on language
                                    qa_text = self._format_qa_pair(question, answer, language)
                                    
                                    chunks.append({
                                        "text": qa_text,
                                        "section": section_title,
                                        "type": "qa_pair",
                                        "metadata": {
                                            "question": question,
                                            "has_question": True,
                                            "qa_format": "implicit"
                                        }
                                    })
                                    
                                    # Mark these sentences as processed
                                    processed_indices.update(range(idx, answer_end_idx + 1))
                    
                    # Process remaining sentences as regular paragraphs
                    remaining_sentences = [s for i, s in enumerate(sentences) if i not in processed_indices]
                    remaining_text = ' '.join(remaining_sentences).strip()
                    
                    if remaining_text:
                        # Check if the remaining text is too long
                        if len(remaining_text.split()) > max_chunk_size:
                            # Split into smaller chunks
                            self._process_text_into_chunks(remaining_text, section_title, chunks, max_chunk_size, language)
                        else:
                            # Add as a single chunk
                            chunks.append({
                                "text": remaining_text,
                                "section": section_title,
                                "type": "paragraph",
                                "metadata": {
                                    "has_question": False
                                }
                            })
        
        return chunks

    def _process_text_into_chunks(self, text, section_title, chunks, max_chunk_size, language):
        """Helper method to split text into appropriate sized chunks"""
        try:
            sentences = nltk.sent_tokenize(text, language if language in ['en', 'es', 'fr', 'de', 'it', 'nl', 'pt'] else 'en')
        except Exception:
            sentences = re.split(r'(?<=[.!?])\s+', text)
            
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            # Rough estimate of words
            sentence_size = len(sentence.split())
            
            if current_size + sentence_size <= max_chunk_size:
                current_chunk.append(sentence)
                current_size += sentence_size
            else:
                # Save current chunk if not empty
                if current_chunk:
                    chunks.append({
                        "text": ' '.join(current_chunk),
                        "section": section_title,
                        "type": "paragraph_chunk",
                        "metadata": {
                            "has_question": False
                        }
                    })
                
                # Start new chunk
                current_chunk = [sentence]
                current_size = sentence_size
        
        # Add final chunk if not empty
        if current_chunk:
            chunks.append({
                "text": ' '.join(current_chunk),
                "section": section_title,
                "type": "paragraph_chunk",
                "metadata": {
                    "has_question": False
                }
            })

    def _format_qa_pair(self, question, answer, language):
        """Format Q&A pair based on language"""
        if language == 'en':
            return f"Q: {question}\nA: {answer}"
        elif language == 'es':
            return f"P: {question}\nR: {answer}"
        elif language == 'fr':
            return f"Q: {question}\nR: {answer}"
        elif language == 'de':
            return f"F: {question}\nA: {answer}"
        elif language == 'it':
            return f"D: {question}\nR: {answer}"
        elif language == 'pt':
            return f"P: {question}\nR: {answer}"
        elif language == 'zh':
            return f"问: {question}\n答: {answer}"
        elif language == 'ja':
            return f"質問: {question}\n回答: {answer}"
        elif language == 'ko':
            return f"질문: {question}\n답변: {answer}"
        elif language == 'ru':
            return f"В: {question}\nО: {answer}"
        else:
            # Default to English format
            return f"Q: {question}\nA: {answer}"

    def _identify_sections(self, text, language=None):
        """
        Identify sections in the document based on headers and structure.
        Language-aware section detection.
        
        Returns a list of (section_title, section_content) dictionaries.
        """
        # Common header patterns (language-agnostic patterns work for most languages)
        header_patterns = [
            r"^#+\s+(.+)$",           # Markdown headers (universal)
            r"^(.+)\n[=]+$",          # Underlined headers with = (universal)
            r"^(.+)\n[-]+$",          # Underlined headers with - (universal)
            r"^(\d+\.\s+.+)$",        # Numbered headers like "1. Introduction" (universal)
            r"^([A-Z0-9][A-Za-z0-9\s]+)[:.]\s*$"  # Title Case followed by colon or period
        ]
        
        # Split text into lines
        lines = text.split('\n')
        
        sections = []
        current_section = {"title": "Introduction", "content": []}
        
        for line in lines:
            # Check if this line is a header
            is_header = False
            for pattern in header_patterns:
                match = re.match(pattern, line)
                if match:
                    # Save current section if it has content
                    if current_section["content"]:
                        sections.append(current_section)
                    
                    # Start new section
                    current_section = {
                        "title": match.group(1).strip(),
                        "content": []
                    }
                    is_header = True
                    break
            
            # If not a header, add to current section
            if not is_header:
                current_section["content"].append(line)
        
        # Add the last section
        if current_section["content"]:
            sections.append(current_section)
        
        # Convert section content lists to strings
        for section in sections:
            section["content"] = '\n'.join(section["content"]).strip()
        
        return sections

    def _identify_qa_pairs(self, text, language=None):
        """
        Identify potential question-answer pairs in text with explicit formatting.
        Language-aware Q&A detection.
        
        Returns a list of (question, answer) dictionaries.
        """
        # Define Q&A patterns based on language
        qa_patterns = []
        
        # English patterns (default)
        en_patterns = [
            r"(?:Q|Question)[:\.]?\s*(.*?)\s*(?:A|Answer)[:\.]?\s*([\s\S]*?)(?=(?:Q|Question)[:\.]|\Z)",
            r"(?:\d+\.\s*)(.*?)\?[\s]+([\s\S]*?)(?=\d+\.\s*.*\?|\Z)",
            r"(?:\*\s*)(.*?)\?[\s]+([\s\S]*?)(?=\*\s*.*\?|\Z)"
        ]
        
        # Add language-specific patterns
        if language == 'es':  # Spanish
            qa_patterns.extend([
                r"(?:P|Pregunta)[:\.]?\s*(.*?)\s*(?:R|Respuesta)[:\.]?\s*([\s\S]*?)(?=(?:P|Pregunta)[:\.]|\Z)",
            ])
        elif language == 'fr':  # French
            qa_patterns.extend([
                r"(?:Q|Question)[:\.]?\s*(.*?)\s*(?:R|Réponse)[:\.]?\s*([\s\S]*?)(?=(?:Q|Question)[:\.]|\Z)",
            ])
        elif language == 'de':  # German
            qa_patterns.extend([
                r"(?:F|Frage)[:\.]?\s*(.*?)\s*(?:A|Antwort)[:\.]?\s*([\s\S]*?)(?=(?:F|Frage)[:\.]|\Z)",
            ])
        elif language == 'zh':  # Chinese
            qa_patterns.extend([
                r"(?:问题|问)[：:]\s*(.*?)\s*(?:答案|答)[：:]\s*([\s\S]*?)(?=(?:问题|问)[：:]|\Z)",
            ])
        elif language == 'ja':  # Japanese
            qa_patterns.extend([
                r"(?:質問|Q)[：:]\s*(.*?)\s*(?:回答|A)[：:]\s*([\s\S]*?)(?=(?:質問|Q)[：:]|\Z)",
            ])
        
        # Always include English patterns as fallback
        qa_patterns.extend(en_patterns)
        
        qa_pairs = []
        for pattern in qa_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            for match in matches:
                if len(match) == 2 and match[0].strip() and match[1].strip():
                    # Format the Q&A text based on language
                    qa_text = self._format_qa_pair(match[0].strip(), match[1].strip(), language)
                    
                    qa_pairs.append({
                        "question": match[0].strip(),
                        "answer": match[1].strip(),
                        "combined": qa_text
                    })
        
        return qa_pairs

    async def process_message(self, message):
        """Process individual message and store in Elasticsearch with vectors"""
        try:
            # Extract text content from message
            content = message.get('content')
            if not content:
                logger.warning("Message has no content")
                return

            tenant_id = message.get('tenant_id')
            metadata = message.get('metadata', {})
            document_id = str(uuid.uuid4())
            
            # Check if language is specified in metadata, otherwise detect it
            language = metadata.get('language')
            if not language:
                language = detect_language(content)
                metadata['language'] = language
            
            # Store original document first (for reference and email templates)
            original_doc = {
                'content': content,
                'tenant_id': tenant_id,
                'metadata': metadata,
                'document_id': document_id
            }
            
            await self.es_client.index(
                index=f"{ES_CONFIG['index_name']}_originals",
                document=original_doc,
                id=document_id
            )
            logger.info(f"Indexed original document with ID: {document_id}")
            
            # Extract semantic chunks with language awareness
            chunks = self._extract_semantic_chunks(content, language)
            logger.info(f"Extracted {len(chunks)} semantic chunks from document")
            
            # Process and index each chunk
            indexing_tasks = []
            for i, chunk in enumerate(chunks):
                # Generate vector embedding for the chunk
                vector = self.st_model.encode(chunk["text"]).tolist()
                
                # Prepare chunk document
                chunk_doc = {
                    'content': chunk["text"],
                    'content_vector': vector,
                    'tenant_id': tenant_id,
                    'document_id': document_id,
                    'chunk_type': chunk["type"],
                    'section_title': chunk["section"],
                    'metadata': {
                        **metadata,
                        **chunk.get("metadata", {}),
                        'chunk_index': i,
                        'total_chunks': len(chunks)
                    }
                }
                
                # Index the chunk
                chunk_id = f"{document_id}_chunk_{i}"
                indexing_tasks.append(
                    self.es_client.index(
                        index=ES_CONFIG['index_name'],
                        document=chunk_doc,
                        id=chunk_id
                    )
                )
            
            # Wait for all indexing tasks to complete
            await asyncio.gather(*indexing_tasks)
            
            logger.info(f"Successfully indexed all {len(chunks)} chunks for document {document_id}")
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)

    def safe_json_deserializer(self, x):
        """Safely deserialize JSON, return None if invalid"""
        try:
            return json.loads(x.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON received: {x}. Error: {e}")
            return {"raw_message": x.decode("utf-8"), "error": str(e)}

    async def send_to_dead_letter_queue(self, message, error):
        """Send problematic messages to a dead letter topic"""
        error_message = {
            "original_message": message,
            "error": error
        }
        
        def delivery_callback(err, msg):
            if err:
                logger.error(f"Failed to send to dead letter queue: {err}")
            else:
                logger.info(f"Message sent to dead_letter_topic")
        
        self.producer.produce("dead_letter_topic", json.dumps(error_message).encode('utf-8'), callback=delivery_callback)
        self.producer.poll(0)  # Trigger delivery callbacks

    async def consume_messages(self):
        """Consume messages from Kafka"""
        consumer = Consumer(self.consumer_config)
        
        try:
            # Subscribe to topic
            consumer.subscribe([self.topic])
            
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        # Error
                        logger.error(f"Error: {msg.error()}")
                    continue
                
                # Process message
                try:
                    value = msg.value()
                    if isinstance(value, bytes):
                        value = json.loads(value.decode('utf-8'))
                    elif isinstance(value, str):
                        value = json.loads(value)
                    
                    logger.info(f"Received message from partition {msg.partition()}, offset {msg.offset()}")
                    await self.process_message(value)
                    logger.info(f"Successfully processed message for tenant {value.get('tenant_id')}")
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}", exc_info=True)
                    await self.send_to_dead_letter_queue(value, str(e))
                    
        except KeyboardInterrupt:
            pass
        finally:
            # Close the consumer
            consumer.close()

    async def run(self):
        """Main processing loop"""
        logger.info("Starting message processing...")
        
        # Initialize producer
        self.producer = Producer(self.producer_config)
        logger.info("Producer initialized successfully")
        
        # Setup Elasticsearch indices
        await self._setup_elasticsearch_indices(ES_CONFIG['index_name'])
        
        # Start consuming messages
        try:
            await self.consume_messages()
        except Exception as e:
            logger.error(f"Fatal error in main loop: {str(e)}", exc_info=True)
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down...")
        
        # Close the HTTP client
        await self.http_client.aclose()
        
        # Close the Elasticsearch client
        await self.es_client.close()
        
        # Ensure all messages are delivered before shutting down producer
        self.producer.flush()
        
        logger.info("Resources closed.")

# Example usage
if __name__ == "__main__":
    # Initialize processor
    processor = MultilingualMessageProcessor()
    logger.info("Initializing multilingual message processor...")
    
    # Run the processor in an asyncio event loop
    asyncio.run(processor.run())