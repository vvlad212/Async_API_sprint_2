from abc import ABC, abstractmethod


class ABSElasticStorage(ABC):

    @abstractmethod
    def get_by_id(self, **kwargs):
        pass

    @abstractmethod
    def search(self, **kwargs):
        pass
