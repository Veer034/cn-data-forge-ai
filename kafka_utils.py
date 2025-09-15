# kafka_utils.py
import json
import datetime
import asyncio
from typing import Dict, Any
from pydantic import BaseModel
from confluent_kafka import Producer
from contextvars import ContextVar

tracking_id_var = ContextVar("X-Tracking-ID", default="NA")

class KafkaUtils:
    """Utilities for Kafka message handling"""
    
    @staticmethod
    async def publish_kafka_message(producer: Producer, topic: str, key: str, message: Any):
        """
        Publish a message to a Kafka topic.
        
        Args:
            producer: Kafka producer instance
            topic: Kafka topic.
            key: Message key.
            message: Message payload (will be JSON serialized).
            
        Returns:
            Future for the message delivery.
        """
        serialized_key = str(key).encode("utf-8")
        
        if isinstance(message, BaseModel):
            # Custom encoder that can handle sets
            def set_encoder(obj):
                if isinstance(obj, set):
                    return list(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            serialized_value = json.dumps(message.model_dump(), default=set_encoder).encode("utf-8")
        else:
            def set_encoder(obj):
                if isinstance(obj, set):
                    return list(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            serialized_value = json.dumps(message, default=set_encoder).encode("utf-8")
        
        # Create an asyncio Future to wait for delivery report
        future = asyncio.Future()
        
        def delivery_callback(err, msg):
            if err:
                future.set_exception(Exception(f"Message delivery failed: {err} for key: {key}"))
            else:
                future.set_result(msg)
        
        # Get current trackingId and ensure it's a string
        tracking_id = tracking_id_var.get() or "NA"
        if isinstance(tracking_id, bytes):
            tracking_id_str = tracking_id.decode("utf-8")
        else:
            tracking_id_str = str(tracking_id)

        producer.produce(
            topic,
            key=serialized_key,
            value=serialized_value,
            callback=delivery_callback,
            headers=[("X-Tracking-ID", tracking_id_str)]
        )
        producer.poll(1)  # Trigger delivery callbacks
        producer.flush()
        
        return await future

    @staticmethod
    async def send_to_dead_letter_queue(producer: Producer, dlq_topic: str, message: Dict[str, Any], error: str):
        """
        Send problematic messages to a dead letter topic.
        
        Args:
            producer: Kafka producer instance
            dlq_topic: Dead letter queue topic
            message: Original message.
            error: Error description.
        """
        # Use epoch timestamp (seconds since epoch) instead of formatted datetime string
        epoch_timestamp = int(datetime.datetime.now().timestamp())
    
        error_message = {
            "originalMessage": message,
            "error": error,
            "eventTime": epoch_timestamp
        }
        
        await KafkaUtils.publish_kafka_message(
            producer,
            dlq_topic,
            message.get('tenantId', 'unknown'),
            error_message
        )

    @staticmethod
    def parse_message(message_value: Any) -> Dict[str, Any]:
        """
        Parse message value into a dictionary.
        
        Args:
            message_value: Raw message value.
            
        Returns:
            Parsed message as a dictionary.
        """
        if isinstance(message_value, bytes):
            try:
                return json.loads(message_value.decode('utf-8'))
            except json.JSONDecodeError as e:
                return {"raw_content": message_value.decode('utf-8', errors='replace')}
        elif isinstance(message_value, str):
            try:
                return json.loads(message_value)
            except json.JSONDecodeError:
                return {"raw_content": message_value}
        elif isinstance(message_value, dict):
            return message_value
        else:
            return {"raw_content": str(message_value)}

class SystemHealthUtils:
    """Utilities for system health checks"""
    
    @staticmethod
    async def test_elasticsearch_connection(es_client) -> bool:
        """Test Elasticsearch connectivity and health"""
        try:
            # Test basic connectivity
            info = await es_client.info()
            print(f"✓ Elasticsearch connection successful")
            print(f"  - Cluster name: {info.get('cluster_name', 'unknown')}")
            print(f"  - Version: {info.get('version', {}).get('number', 'unknown')}")
            
            # Test cluster health
            health = await es_client.cluster.health()
            status = health.get('status', 'unknown')
            print(f"  - Cluster status: {status}")
            
            if status in ['green', 'yellow']:
                print("✓ Elasticsearch cluster is healthy")
                return True
            else:
                print(f"⚠ Elasticsearch cluster status is: {status}")
                return False
                
        except Exception as e:
            print(f"✗ Elasticsearch connection test failed: {e}")
            return False

    @staticmethod
    async def test_kafka_connectivity(producer_config: Dict[str, Any], required_topics: set) -> bool:
        """Test Kafka connectivity"""
        print("Testing Kafka connectivity...")
        
        # Test producer connectivity
        try:
            test_producer = Producer(producer_config)
            # Get metadata to test connectivity
            metadata = test_producer.list_topics(timeout=10)
            print(f"✓ Kafka producer connection successful")
            print(f"  - Available topics: {len(metadata.topics)} topics found")
            
            # Check if our required topics exist
            available_topics = set(metadata.topics.keys())
            
            missing_topics = required_topics - available_topics
            if missing_topics:
                print(f"⚠ Missing required topics: {missing_topics}")
                print("Topics will be auto-created if Kafka allows it")
            else:
                print("✓ All required Kafka topics are available")
            
            test_producer.flush()
            return True
            
        except Exception as e:
            print(f"✗ Kafka connectivity test failed: {e}")
            return False