from abc import ABC, abstractmethod

class CosmosdbInterface(ABC):
    @abstractmethod
    def save(self, data):
        pass

    @abstractmethod
    def get_all(self, year):
        pass

    @abstractmethod
    def get_available_years(self, status: str):
        pass

    @abstractmethod
    def update(self, id:str, data):
        pass

    @abstractmethod
    def get_one(self, id: str):
        pass