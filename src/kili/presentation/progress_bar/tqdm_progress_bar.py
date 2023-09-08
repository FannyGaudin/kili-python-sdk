from typing import Optional

from tqdm import tqdm

from .abstract_progress_bar import AbstractProgressBar


class TqdmProgressBar(AbstractProgressBar):
    def __init__(self, description: str, disable: Optional[bool] = False):
        self._progress_bar = tqdm(desc=description, colour="#ff8200", ascii="░▒█", disable=disable)

    def update(self, value: int):
        self._progress_bar.update(value)

    def close(self):
        self._progress_bar.close()

    def set_total(self, total: int):
        self._progress_bar.total = total
