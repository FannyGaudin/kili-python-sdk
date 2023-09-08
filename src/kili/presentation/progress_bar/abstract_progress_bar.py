import abc


class AbstractProgressBar(abc.ABC):
    @abc.abstractmethod
    def __init__(self, description: str) -> None:
        pass

    @abc.abstractmethod
    def update(self, value: int):
        pass

    @abc.abstractmethod
    def set_total(self, total: int):
        pass
