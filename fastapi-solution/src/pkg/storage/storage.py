from abc import ABC, abstractmethod


class ABSStorage(ABC):

    @abstractmethod
    def get_by_id(self, **kwargs):
        pass

    @abstractmethod
    def search(self, **kwargs):
        pass
