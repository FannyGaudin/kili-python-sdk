"""Abstract class for label parsing."""
from abc import ABC, abstractmethod
from pathlib import Path

from kili.domain.label_importer import KiliDataToImport


class AbstractLabelingDataParser(ABC):
    """Abstract class for label parsing."""

    def __init__(self) -> None:
        """Initializes the parser."""
        pass

    @abstractmethod
    def parse(self, label_file: Path) -> KiliDataToImport:
        """Parses a label file."""
