from abc import ABC
from logging import Logger
from typing import Dict, Any


class AbstractLogger(ABC):

    def __init__(self, disable_tqdm: bool, logger_params: Dict[str, Any]) -> None:
        self.disable_tqdm = disable_tqdm
        self.logger =  Logger(**logger_params)


class SDKLogger(AbstractLogger):
    pass
