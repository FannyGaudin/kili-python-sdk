import abc
from typing import Optional


class AbstractProgressBar(abc.ABC):
    @abc.abstractmethod
    def __init__(self, description: str, disable: Optional[bool]) -> None:
        pass

    @abc.abstractmethod
    def update(self, value: int):
        pass

    @abc.abstractmethod
    def set_total(self, total: int):
        pass
