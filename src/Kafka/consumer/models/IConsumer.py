from kafka import KafkaConsumer
from abc import ABC, abstractmethod

import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_URL = os.environ['KAFKA_URL']

class IConsumer(ABC):
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_URL], 
            auto_offset_reset='earliest', 
            enable_auto_commit=False,
            value_deserializer=lambda x: x.decode('utf-8')
        )
    
    def _sub_with_topics(self, topics):
        self.consumer.subscribe(topics=topics)

    @abstractmethod
    def _sub(self):
        pass
    
    @abstractmethod
    def _on_new_message(self, message):
        pass
    
    @abstractmethod
    def main(self):
        pass


