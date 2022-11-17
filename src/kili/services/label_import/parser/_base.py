"""
Parser classes
"""
from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

from kili.services.label_import.types import Classes


class AbstractLabelParser:  # pylint: disable=too-few-public-methods
    """
    Abstract label parser
    """

    def __init__(self, classes_by_id: Optional[Classes], target_job: Optional[str]) -> None:
        self.classes_by_id = classes_by_id
        self.target_job = target_job

    @abstractmethod
    def parse(self, label_file: Path) -> Dict[str, Any]:
        """
        Parses a label file
        """
