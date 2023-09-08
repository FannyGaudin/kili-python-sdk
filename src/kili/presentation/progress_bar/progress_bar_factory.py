from typing import Optional

from kili.presentation.progress_bar.tqdm_progress_bar import TqdmProgressBar


class ProgressBarFactory:
    def __init__(self):
        pass
        # self._disable = disable

    def create(self, description: str, type: str = "tqdm"):
        if type == "tqdm":
            return TqdmProgressBar(description, disable=self._disable)

    @property
    def disable(self):
        return self._disable

    @disable.setter
    def disable(self, disable: Optional[bool]):
        self._disable = disable
