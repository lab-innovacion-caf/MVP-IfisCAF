from abc import ABC, abstractmethod

class CosmosdbLoggingInterface(ABC):
    @abstractmethod
    def save_log(self,data):
        pass

    @abstractmethod
    def get_all(self):
        pass