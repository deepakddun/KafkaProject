"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://kafka0:9092",
            "schema_registry_url": "http://schema-registry:8081"

        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            print("INSIDE create topic ")
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": self.broker_properties.get("schema_registry_url")})
        self.producer = AvroProducer({"bootstrap.servers": self.broker_properties.get("bootstrap.servers")},
                                     default_key_schema=self.key_schema,
                                     default_value_schema=self.value_schema

                                     )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #

        # 1. Create an adminclient object
        print("INSIDE create topic method")
        conf = {'bootstrap.servers': self.broker_properties.get('bootstrap.servers')}
        admin = AdminClient(conf)

        # 2 list all topics which returns the dictionary of topics
        topics1 = admin.list_topics(timeout=5).topics
        print(topics1)
        # 3 check if the topic is alreday present in broker . If No then create the topic else skip the topic creation
        if topics1.get(self.topic_name) is not None:
            topic = NewTopic(self.topic_name, self.num_partitions, self.num_replicas)
            # createtopic accept the list and is async process
            outcome = admin.create_topics([topic])
            for topic, f in outcome.items():
                try:
                    f.result()
                    print("Topic {} created".format(topic))
                except Exception as e:
                    print("Failed to create topic {}: {}".format(topic, e))
                    logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        try:
            # according to kafka document https://confluent-kafka-python.readthedocs.io/en/latest/#producer
            # flush will wait for messages in the producer queue to be delivered
            self.producer.flush()
        except Exception as e:
            logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
