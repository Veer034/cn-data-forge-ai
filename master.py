# main.py
"""
Main application runner that brings together all the utility modules
to create a complete multilingual document processing system.
"""

import asyncio
import sys
from typing import Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError
from contextvars import ContextVar

# Import all our utility modules
from multilingual_processor import MultilingualMessageProcessor, tracking_id_var
from kafka_utils import KafkaUtils, SystemHealthUtils
from config import KAFKA_CONFIG, ES_CONFIG
from logger_config import get_logger

logger = get_logger(__name__)

class MultilingualProcessorApplication:
    """Main application class that orchestrates the entire system"""
    
    def __init__(self):
        self.processor = None
        self.shutdown_requested = False
        
    async def initialize(self):
        """Initialize the processor and all its components"""
        try:
            # Initialize the main processor
            self.processor = MultilingualMessageProcessor(ES_CONFIG, KAFKA_CONFIG)
            
            logger.info("=" * 60)
            logger.info("STARTING MULTILINGUAL MESSAGE PROCESSOR")
            logger.info("=" * 60)
            
            startup_success = True
            
            # Test Elasticsearch connectivity
            if not await SystemHealthUtils.test_elasticsearch_connection(self.processor.es_client):
                startup_success = False
                logger.error("✗ STARTUP FAILED: Elasticsearch connectivity check failed")
            
            # Initialize Kafka producer
            logger.info("Initializing Kafka producer...")
            try:
                self.processor.producer = Producer(self.processor.producer_config)
                logger.info("✓ Kafka producer initialized successfully")
                
                # Test Kafka connectivity
                required_topics = {
                    self.processor.document_request_topic,
                    self.processor.document_dlq_topic,
                    self.processor.faq_request_topic,
                    self.processor.faq_dlq_topic,
                    self.processor.response_topic
                }
                
                if not await SystemHealthUtils.test_kafka_connectivity(
                    self.processor.producer_config, required_topics):
                    startup_success = False
                    logger.error("✗ STARTUP FAILED: Kafka connectivity check failed")
                    
            except Exception as e:
                startup_success = False
                logger.error(f"✗ STARTUP FAILED: Kafka producer initialization failed: {e}")
            
            # Setup Elasticsearch indices
            try:
                await self.processor._setup_elasticsearch_indices()
            except Exception as e:
                startup_success = False
                logger.error(f"✗ STARTUP FAILED: Elasticsearch setup failed: {e}")
            
            if not startup_success:
                logger.error("=" * 60)
                logger.error("SYSTEM STARTUP FAILED - CRITICAL ERRORS DETECTED")
                logger.error("Please check the logs above and fix connectivity issues")
                logger.error("=" * 60)
                raise Exception("System startup failed due to connectivity issues")
            
            # Log successful startup
            logger.info("=" * 60)
            logger.info("🎉 SYSTEM STARTUP SUCCESSFUL! 🎉")
            logger.info("✓ All connectivity checks passed")
            logger.info("✓ Elasticsearch: Connected and healthy")
            logger.info("✓ Kafka: Producer and topics verified")
            logger.info("✓ SentenceTransformer: Model loaded")
            logger.info("✓ Language Detection: Ready")
            logger.info("=" * 60)
            logger.info("System is now ready to process messages...")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize processor: {e}")
            return False

    async def run_document_consumer(self):
        """Run the document consumer loop"""
        logger.info(f"Starting document consumer for topic: {self.processor.document_request_topic}")
        consumer = Consumer(self.processor.consumer_config)
        
        try:
            consumer.subscribe([self.processor.document_request_topic])
            logger.info(f"✓ Document consumer subscribed to topic: {self.processor.document_request_topic}")
            
            while not self.processor.shutdown_requested:
                msg = consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Document consumer error: {msg.error()}")
                    continue
                
                # Set the tracking ID from Kafka headers before logging
                kafka_headers = dict(msg.headers() or [])
                tracking_id = kafka_headers.get('X-Tracking-ID', b'NA')
                tracking_id_var.set(tracking_id.decode('utf-8') if isinstance(tracking_id, bytes) else str(tracking_id))

                # Process message
                try:
                    value = KafkaUtils.parse_message(msg.value())
                    logger.info(f"📄 Processing document message (partition: {msg.partition()}, offset: {msg.offset()})")
                    await self.processor.process_document_message_simplified(value)
                    logger.info("✓ Document message processed successfully")
                    
                except Exception as e:
                    logger.error(f"✗ Error processing document message: {str(e)}", exc_info=True)
                    await KafkaUtils.send_to_dead_letter_queue(
                        self.processor.producer,
                        self.processor.document_dlq_topic,
                        KafkaUtils.parse_message(msg.value()) if msg.value() else {},
                        str(e)
                    )
                    
        except Exception as e:
            logger.error(f"✗ CRITICAL: Document consumer error: {e}", exc_info=True)
            raise
        finally:
            consumer.close()
            logger.info("📄 Document Kafka consumer closed")

    async def run_faq_consumer(self):
        """Run the FAQ consumer loop"""
        logger.info(f"Starting FAQ consumer for topic: {self.processor.faq_request_topic}")
        consumer = Consumer(self.processor.consumer_config)
        
        try:
            consumer.subscribe([self.processor.faq_request_topic])
            logger.info(f"✓ FAQ consumer subscribed to topic: {self.processor.faq_request_topic}")
            
            while not self.processor.shutdown_requested:
                msg = consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"FAQ consumer error: {msg.error()}")
                    continue
                
                # Process message
                try:
                    value = KafkaUtils.parse_message(msg.value())
                    logger.info(f"❓ Processing FAQ message (partition: {msg.partition()}, offset: {msg.offset()})")
                    await self.processor.process_faq_message(value)
                    logger.info("✓ FAQ message processed successfully")
                    
                except Exception as e:
                    logger.error(f"✗ Error processing FAQ message: {str(e)}", exc_info=True)
                    await KafkaUtils.send_to_dead_letter_queue(
                        self.processor.producer,
                        self.processor.faq_dlq_topic,
                        KafkaUtils.parse_message(msg.value()) if msg.value() else {},
                        str(e)
                    )
                    
        except Exception as e:
            logger.error(f"✗ CRITICAL: FAQ consumer error: {e}", exc_info=True)
            raise
        finally:
            consumer.close()
            logger.info("❓ FAQ Kafka consumer closed")

    async def run(self):
        """Main application run method"""
        try:
            # Initialize the system
            if not await self.initialize():
                logger.error("Failed to initialize the system")
                return False
            
            # Start consuming messages concurrently
            await asyncio.gather(
                self.run_document_consumer(),
                self.run_faq_consumer()
            )
            
        except KeyboardInterrupt:
            logger.info("⚠ Keyboard interrupt received, shutting down gracefully...")
        except Exception as e:
            logger.error(f"✗ CRITICAL: Fatal error in main loop: {str(e)}", exc_info=True)
            logger.error("=" * 60)
            logger.error("SYSTEM ENCOUNTERED A FATAL ERROR")
            logger.error("=" * 60)
            raise
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown of resources"""
        logger.info("=" * 40)
        logger.info("INITIATING GRACEFUL SHUTDOWN")
        logger.info("=" * 40)
        
        try:
            if self.processor:
                # Close the Elasticsearch client
                logger.info("Closing Elasticsearch connection...")
                await self.processor.es_client.close()
                logger.info("✓ Elasticsearch connection closed")
                
                # Ensure all messages are delivered before shutting down producer
                if self.processor.producer:
                    logger.info("Flushing Kafka producer...")
                    self.processor.producer.flush()
                    logger.info("✓ Kafka producer flushed")
            
            logger.info("=" * 40)
            logger.info("✓ GRACEFUL SHUTDOWN COMPLETED")
            logger.info("=" * 40)
            
        except Exception as e:
            logger.error(f"✗ Error during shutdown: {e}")

    # Add the missing methods from the original processor
    async def publish_kafka_message(self, topic: str, key: str, message: Any):
        """Wrapper method for Kafka message publishing"""
        return await KafkaUtils.publish_kafka_message(self.processor.producer, topic, key, message)
    
    # Update the processor methods to use the utility functions
    def _update_processor_methods(self):
        """Update processor methods to use utility functions"""
        # Replace the processor's methods with utility function calls
        self.processor.publish_kafka_message = self.publish_kafka_message
        self.processor._parse_message = KafkaUtils.parse_message
        self.processor.send_to_dead_letter_queue = lambda dlq_topic, message, error: \
            KafkaUtils.send_to_dead_letter_queue(self.processor.producer, dlq_topic, message, error)

async def main():
    """Main entry point"""
    app = MultilingualProcessorApplication()
    
    try:
        await app.run()
        return 0
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1

if __name__ == "__main__":
    # Initialize and run the application
    try:
        logger.info("🚀 Starting Multilingual Message Processor Application")
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Application failed to start: {e}", exc_info=True)
        sys.exit(1)