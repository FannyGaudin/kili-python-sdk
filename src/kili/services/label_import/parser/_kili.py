"""
Kili parsers
"""
import json
from pathlib import Path
from typing import Any, Dict

from kili.services.label_import.parser._base import AbstractLabelParser


class KiliRawLabelParser(AbstractLabelParser):  # pylint: disable=too-few-public-methods
    """
    Kili raw label parser
    """

    def parse(self, label_file: Path) -> Dict[str, Any]:
        with label_file.open("r", encoding="utf-8") as l_f:
            return json.load(l_f)
