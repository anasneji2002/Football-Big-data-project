from abc import ABC, abstractmethod

class IDataProcessor(ABC):
    @abstractmethod
    def processData(self, data):
        pass