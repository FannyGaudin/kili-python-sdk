from kili.presentation.progress_bar.tqdm_progress_bar import TqdmProgressBar


class ProgressBarFactory:
    def create(self, description: str, type: str = "tqdm"):
        if type == "tqdm":
            return TqdmProgressBar(description)
