from tqdm import tqdm


class TqdmProgressBar:
    def __init__(self, description: str):
        self._progress_bar = tqdm(desc=description, colour="#ff8200", ascii="░▒█")

    def update(self, value: int):
        self._progress_bar.update(value)

    def close(self):
        self._progress_bar.close()

    def set_total(self, total: int):
        self._progress_bar.total = total
