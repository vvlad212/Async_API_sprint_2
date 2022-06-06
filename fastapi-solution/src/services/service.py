from abc import ABC, abstractmethod

from fastapi import Query
from pydantic import BaseModel


class ABSService(ABC):

    @abstractmethod
    def get_by_id(self, **kwargs):
        pass

    @abstractmethod
    def get_list(self, **kwargs):
        pass
