from abc import ABC, abstractmethod
from kafka import KafkaProducer


class IProducer(ABC):
    def __init__(self):
        self.__producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"]
        )

    def _pub_with_topic(self, val, topic):
        self.__producer.send(topic, value=val.encode("utf-8"))
    
    @abstractmethod
    def _pub(self, val):
        pass
    
    @abstractmethod
    def main(self):
        pass