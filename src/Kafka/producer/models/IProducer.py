from abc import ABC, abstractmethod
from kafka import KafkaProducer

import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_URL = os.environ['KAFKA_URL']

class IProducer(ABC):
    def __init__(self):
        self.__producer = KafkaProducer(
            bootstrap_servers=[KAFKA_URL]
        )

    def _pub_with_topic(self, val, topic):
        self.__producer.send(topic, value=val.encode("utf-8"))
    
    @abstractmethod
    def _pub(self, val):
        pass
    
    @abstractmethod
    def main(self):
        pass