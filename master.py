"""
Enhanced main application runner with parallel processing capabilities
similar to MultilingualMessageProcessor architecture.
"""

import asyncio
import sys
from datetime import datetime
import psutil
import os
import signal
import uuid
from typing import Dict, Any, Set
from confluent_kafka import Consumer, Producer, KafkaError
from contextvars import ContextVar
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

# Import all our utility modules
from multilingual_processor import MultilingualMessageProcessor, tracking_id_var
from kafka_utils import KafkaUtils, SystemHealthUtils
from config import KAFKA_CONFIG, ES_CONFIG
from logger_config import get_logger

logger = get_logger(__name__)

@dataclass
class SystemConfig:
    """System configuration based on hardware capabilities"""
    cpu_cores: int
    has_gpu: bool
    thread_pool_size: int
    kafka_poll_timeout: float
    max_concurrent_messages: int = 3  # Fixed to 3 for simplicity

class HardwareDetector:
    """Detect system capabilities and configure accordingly"""
    
    @staticmethod
    def detect_system_config() -> SystemConfig:
        """Detect system capabilities and create optimal configuration"""
        cpu_cores = psutil.cpu_count(logical=False) or 4
        has_gpu = False
        try:
            import torch
            has_gpu = torch.cuda.is_available()
            if has_gpu:
                logger.info(f"GPU detected: CUDA available")
        except ImportError:
            logger.info("No GPU libraries available")
        
        config = SystemConfig(
            cpu_cores=cpu_cores,
            has_gpu=has_gpu,
            thread_pool_size=min(cpu_cores, 6),
            kafka_poll_timeout=0.1,
            max_concurrent_messages=3  # Fixed to 3
        )
        
        logger.info(f"System Config - CPU Cores: {cpu_cores}, GPU: {has_gpu}")
        logger.info(f"Max Concurrent Messages: 3 (fixed)")
        logger.info(f"Thread Pool: {config.thread_pool_size}")
        
        return config

class MultilingualProcessorApplication:
    """Enhanced main application class with parallel processing capabilities"""
    
    def __init__(self):
        logger.info("=" * 60)
        logger.info("INITIALIZING ENHANCED MULTILINGUAL PROCESSOR APPLICATION")
        logger.info("=" * 60)
        
        # Detect system capabilities
        self.system_config = HardwareDetector.detect_system_config()
        
        # Log system information
        logger.info(f"Server startup time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Process ID: {os.getpid()}")
        
        self.processor = None
        self.shutdown_requested = False
        self.stop_event: Optional[asyncio.Event] = None  # will be created when loop is available
     
        
        # Thread pool for CPU-bound operations
        self.thread_pool = ThreadPoolExecutor(max_workers=self.system_config.thread_pool_size)
        logger.info(f"Thread pool initialized with {self.system_config.thread_pool_size} workers")
        
        # Semaphores to control concurrent message processing
        self.document_processing_semaphore = asyncio.Semaphore(self.system_config.max_concurrent_messages)
        self.faq_processing_semaphore = asyncio.Semaphore(self.system_config.max_concurrent_messages)
        
        # Track processing tasks for graceful shutdown
        self.active_document_tasks: Set[asyncio.Task] = set()
        self.active_faq_tasks: Set[asyncio.Task] = set()
        
        logger.info("Signal handlers configured for graceful shutdown")
        
        # Statistics
        self.document_processed_count = 0
        self.document_failed_count = 0
        self.faq_processed_count = 0
        self.faq_failed_count = 0
        self.document_concurrent_peak = 0
        self.faq_concurrent_peak = 0
        

    async def setup_signal_handlers(self):
        """Register asyncio-friendly signal handlers on the running loop."""
        loop = asyncio.get_running_loop()
        if self.stop_event is None:
            self.stop_event = asyncio.Event()

        # guard to log only on first signal
        first_signal = {"seen": False}

        def _sync_on_signal(sig_name):
            if not first_signal["seen"]:
                logger.info(f"Received signal {sig_name}, initiating graceful shutdown...")
                first_signal["seen"] = True
            # mark request
            self.shutdown_requested = True
            # set event so tasks waiting on it will wake
            if not self.stop_event.is_set():
                self.stop_event.set()

        # Use loop.add_signal_handler for SIGINT and SIGTERM
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: _sync_on_signal(s))
            except NotImplementedError:
                # Windows / event loop limitations: fallback to signal.signal
                signal.signal(sig, lambda s, f: _sync_on_signal(s))


    def _signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        self.shutdown_requested = True
        
    async def initialize(self):
        """Initialize the processor and all its components"""
        try:
            # Initialize the main processor
            self.processor = MultilingualMessageProcessor(ES_CONFIG, KAFKA_CONFIG)
            
            logger.info("=" * 60)
            logger.info("STARTING ENHANCED MULTILINGUAL MESSAGE PROCESSOR")
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
            logger.info(f"✓ Max concurrent document processing: {self.system_config.max_concurrent_messages}")
            logger.info(f"✓ Max concurrent FAQ processing: {self.system_config.max_concurrent_messages}")
            logger.info(f"✓ Thread pool size: {self.system_config.thread_pool_size}")
            logger.info(f"✓ GPU acceleration: {self.system_config.has_gpu}")
            logger.info("=" * 60)
            logger.info("System is now ready to process messages...")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize processor: {e}")
            return False

    async def process_document_message_with_semaphore(self, msg_data: dict, msg_key: str, msg_headers: dict):
        """Process single document message with concurrency control"""
        async with self.document_processing_semaphore:
            try:
                # Set tracking ID for this task
                tracking_id = msg_headers.get('X-Tracking-ID', b'NA')
                if isinstance(tracking_id, bytes):
                    tracking_id = tracking_id.decode('utf-8')
                tracking_id_var.set(str(tracking_id))
                
                # Process the document message
                await self.processor.process_document_message_simplified(msg_data)
                self.document_processed_count += 1
                
            except Exception as e:
                self.document_failed_count += 1
                request_id = str(uuid.uuid4())
                logger.error(f"Error processing document message | Request ID: {request_id} | Error: {str(e)}", exc_info=True)
                await self.send_to_document_dead_letter_queue(request_id, msg_data, str(e))

    async def process_faq_message_with_semaphore(self, msg_data: dict, msg_key: str, msg_headers: dict):
        """Process single FAQ message with concurrency control"""
        async with self.faq_processing_semaphore:
            try:
                # Set tracking ID for this task
                tracking_id = msg_headers.get('X-Tracking-ID', b'NA')
                if isinstance(tracking_id, bytes):
                    tracking_id = tracking_id.decode('utf-8')
                tracking_id_var.set(str(tracking_id))
                
                # Process the FAQ message
                await self.processor.process_faq_message(msg_data)
                self.faq_processed_count += 1
                
            except Exception as e:
                self.faq_failed_count += 1
                request_id = str(uuid.uuid4())
                logger.error(f"Error processing FAQ message | Request ID: {request_id} | Error: {str(e)}", exc_info=True)
                await self.send_to_faq_dead_letter_queue(request_id, msg_data, str(e))

    async def send_to_document_dead_letter_queue(self, request_id: str, message: dict, error: str):
        """Send problematic document messages to dead letter queue"""
        await KafkaUtils.send_to_dead_letter_queue(
            self.processor.producer,
            self.processor.document_dlq_topic,
            message,
            error,
            request_id
        )

    async def send_to_faq_dead_letter_queue(self, request_id: str, message: dict, error: str):
        """Send problematic FAQ messages to dead letter queue"""
        await KafkaUtils.send_to_dead_letter_queue(
            self.processor.producer,
            self.processor.faq_dlq_topic,
            message,
            error,
            request_id
        )


    async def run_document_consumer(self):
        """Run document consumer loop with graceful shutdown"""
        logger.info(f"Starting document consumer for topic: {self.processor.document_request_topic}")
        consumer = Consumer(self.processor.consumer_config)
        loop = asyncio.get_running_loop()

        try:
            consumer.subscribe([self.processor.document_request_topic])
            logger.info(f"✓ Document consumer subscribed to topic: {self.processor.document_request_topic}")

            message_count = 0
            last_heartbeat = datetime.now()

            while not self.shutdown_requested:
                if self.stop_event is not None and self.stop_event.is_set():
                    logger.info("Shutdown requested, stopping consumption of new document messages")
                    break

                msg = await loop.run_in_executor(None, consumer.poll, self.system_config.kafka_poll_timeout)

                if msg is None:
                    completed_tasks = [t for t in self.active_document_tasks if t.done()]
                    for task in completed_tasks:
                        self.active_document_tasks.remove(task)
                        try:
                            await task
                            consumer.commit()
                        except Exception as e:
                            logger.error(f"Document task completed with error: {e}")
                    await asyncio.sleep(0.01)
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Document consumer error: {msg.error()}")
                    continue

                try:
                    value = KafkaUtils.parse_message(msg.value())
                    message_count += 1
                    tenant_id = value.get("tenantId", "unknown")
                    kafka_headers = dict(msg.headers() or [])

                    logger.info(f"📄 Processing document message #{message_count} | Tenant: {tenant_id}")

                    task = asyncio.create_task(
                        self.process_document_message_with_semaphore(
                            value,
                            msg.key().decode("utf-8") if msg.key() else str(tenant_id),
                            kafka_headers
                        )
                    )
                    self.active_document_tasks.add(task)

                except Exception as e:
                    logger.error(f"Error creating document processing task #{message_count}: {e}", exc_info=True)

        finally:
            if self.active_document_tasks:
                logger.info(f"Waiting for {len(self.active_document_tasks)} active document tasks to finish...")
                completed, pending = await asyncio.wait(self.active_document_tasks, return_when=asyncio.ALL_COMPLETED)
                logger.info(f"Completed {len(completed)} document tasks")
            try:
                consumer.close()
            except Exception:
                logger.exception("Error closing document consumer")
            logger.info("📄 Document Kafka consumer closed")


    async def run_faq_consumer(self):
        """Run FAQ consumer loop with graceful shutdown"""
        logger.info(f"Starting FAQ consumer for topic: {self.processor.faq_request_topic}")
        consumer = Consumer(self.processor.consumer_config)
        loop = asyncio.get_running_loop()

        try:
            consumer.subscribe([self.processor.faq_request_topic])
            logger.info(f"✓ FAQ consumer subscribed to topic: {self.processor.faq_request_topic}")

            message_count = 0
            last_heartbeat = datetime.now()

            while not self.shutdown_requested:
                if self.stop_event is not None and self.stop_event.is_set():
                    logger.info("Shutdown requested, stopping consumption of new FAQ messages")
                    break

                msg = await loop.run_in_executor(None, consumer.poll, self.system_config.kafka_poll_timeout)

                if msg is None:
                    completed_tasks = [t for t in self.active_faq_tasks if t.done()]
                    for task in completed_tasks:
                        self.active_faq_tasks.remove(task)
                        try:
                            await task
                            consumer.commit()
                        except Exception as e:
                            logger.error(f"FAQ task completed with error: {e}")
                    await asyncio.sleep(0.01)
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"FAQ consumer error: {msg.error()}")
                    continue

                try:
                    value = KafkaUtils.parse_message(msg.value())
                    message_count += 1
                    tenant_id = value.get("tenantId", "unknown")
                    kafka_headers = dict(msg.headers() or [])

                    logger.info(f"❓ Processing FAQ message #{message_count} | Tenant: {tenant_id}")

                    task = asyncio.create_task(
                        self.process_faq_message_with_semaphore(
                            value,
                            msg.key().decode("utf-8") if msg.key() else str(tenant_id),
                            kafka_headers
                        )
                    )
                    self.active_faq_tasks.add(task)

                except Exception as e:
                    logger.error(f"Error creating FAQ processing task #{message_count}: {e}", exc_info=True)

        finally:
            if self.active_faq_tasks:
                logger.info(f"Waiting for {len(self.active_faq_tasks)} active FAQ tasks to finish...")
                completed, pending = await asyncio.wait(self.active_faq_tasks, return_when=asyncio.ALL_COMPLETED)
                logger.info(f"Completed {len(completed)} FAQ tasks")
            try:
                consumer.close()
            except Exception:
                logger.exception("Error closing FAQ consumer")
            logger.info("❓ FAQ Kafka consumer closed")



    async def run(self):
        """Main application run method with parallel processing"""
        try:
            # Initialize the system
            if not await self.initialize():
                logger.error("Failed to initialize the system")
                return False
            
            await self.setup_signal_handlers()
            
            logger.info("=" * 60)
            logger.info("SERVER STARTED SUCCESSFULLY!")
            logger.info(f"Listening for document messages on topic: {self.processor.document_request_topic}")
            logger.info(f"Listening for FAQ messages on topic: {self.processor.faq_request_topic}")
            logger.info(f"Consumer group: {self.processor.consumer_config['group.id']}")
            logger.info(f"Max concurrent document processing: {self.system_config.max_concurrent_messages}")
            logger.info(f"Max concurrent FAQ processing: {self.system_config.max_concurrent_messages}")
            logger.info(f"Thread pool size: {self.system_config.thread_pool_size}")
            logger.info(f"GPU enabled: {self.system_config.has_gpu}")
            logger.info(f"Server ready at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
            # Start consuming messages concurrently with parallel processing
            await asyncio.gather(
                self.run_document_consumer(),
                self.run_faq_consumer(),
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
        """Graceful shutdown with cleanup and statistics"""
        logger.info("=" * 60)
        logger.info("INITIATING GRACEFUL SHUTDOWN")
        logger.info("=" * 60)
        
        try:
            # Signal shutdown to stop accepting new messages
            self.shutdown_requested = True
            
            # Wait for active document tasks to complete
            if self.active_document_tasks:
                logger.info(f"Waiting for {len(self.active_document_tasks)} active document tasks to complete...")
                completed, pending = await asyncio.wait(
                    self.active_document_tasks,
                    timeout=60.0,
                    return_when=asyncio.ALL_COMPLETED
                )
                
                # Cancel remaining tasks if any
                for task in pending:
                    task.cancel()
                
                logger.info(f"Completed {len(completed)} document tasks, cancelled {len(pending)} tasks")
            
            # Wait for active FAQ tasks to complete
            if self.active_faq_tasks:
                logger.info(f"Waiting for {len(self.active_faq_tasks)} active FAQ tasks to complete...")
                completed, pending = await asyncio.wait(
                    self.active_faq_tasks,
                    timeout=60.0,
                    return_when=asyncio.ALL_COMPLETED
                )
                
                # Cancel remaining tasks if any
                for task in pending:
                    task.cancel()
                
                logger.info(f"Completed {len(completed)} FAQ tasks, cancelled {len(pending)} tasks")
                
            if self.processor:
                # Close the Elasticsearch client
                logger.info("Closing Elasticsearch connection...")
                await self.processor.es_client.close()
                logger.info("✓ Elasticsearch connection closed")
                
                # Ensure all messages are delivered before shutting down producer
                if self.processor.producer:
                    logger.info("Flushing Kafka producer...")
                    self.processor.producer.flush(timeout=10)
                    logger.info("✓ Kafka producer flushed")
            
            # Shutdown thread pool
            logger.info("Shutting down thread pool...")
            self.thread_pool.shutdown(wait=True)
            logger.info("Thread pool shutdown completed")
            
            # Print final statistics
            total_processed = self.document_processed_count + self.faq_processed_count
            total_failed = self.document_failed_count + self.faq_failed_count
            
            logger.info("=" * 50)
            logger.info("FINAL PROCESSING STATISTICS")
            logger.info(f"Document messages processed: {self.document_processed_count}")
            logger.info(f"Document processing failures: {self.document_failed_count}")
            logger.info(f"FAQ messages processed: {self.faq_processed_count}")
            logger.info(f"FAQ processing failures: {self.faq_failed_count}")
            logger.info(f"Total messages processed: {total_processed}")
            logger.info(f"Total failures: {total_failed}")
            logger.info(f"Document peak concurrent processing: {self.document_concurrent_peak}")
            logger.info(f"FAQ peak concurrent processing: {self.faq_concurrent_peak}")
            if total_processed + total_failed > 0:
                success_rate = (total_processed / (total_processed + total_failed) * 100)
                logger.info(f"Overall success rate: {success_rate:.2f}%")
            else:
                logger.info("Overall success rate: N/A")
            
            logger.info("=" * 60)
            logger.info("✓ GRACEFUL SHUTDOWN COMPLETED")
            logger.info(f"Shutdown completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"✗ Error during shutdown: {e}", exc_info=True)

    # Wrapper methods for Kafka message handling
    async def publish_kafka_message(self, topic: str, key: str, message: Any):
        """Wrapper method for Kafka message publishing"""
        return await KafkaUtils.publish_kafka_message(self.processor.producer, topic, key, message)

async def main():
    """Main entry point"""
    logger.info("=" * 80)
    logger.info("ENHANCED MULTILINGUAL MESSAGE PROCESSOR - STARTING UP")
    logger.info("=" * 80)
    
    app = MultilingualProcessorApplication()
    
    try:
        # Use uvloop for better performance if available
        try:
            import uvloop
            uvloop.install()
            logger.info("Using uvloop for enhanced performance")
        except ImportError:
            logger.info("uvloop not available, using default event loop")
            
        await app.run()
        return 0
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1

if __name__ == "__main__":
    # Initialize and run the application
    try:
        logger.info("🚀 Starting Enhanced Multilingual Message Processor Application")
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Application failed to start: {e}", exc_info=True)
        sys.exit(1)