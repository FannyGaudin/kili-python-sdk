"""
Base class for all formatters
"""

from abc import ABC, abstractmethod
from typing import List, NamedTuple, Optional

from kili.orm import AnnotationFormat
from kili.services.conversion.typing import ExportType, SplitOption


class ExportParams(NamedTuple):
    """
    Contains all parameters that change the result of the export
    """

    assets_ids: Optional[List[str]]
    export_type: ExportType
    project_id: str
    label_format: AnnotationFormat
    split_option: SplitOption
    output_file: str


class LoggerParams(NamedTuple):
    """
    Contains all parameters related tologging
    """

    user_email: Optional[str]
    disable_tqdm: bool


class BaseFormatter(ABC):
    # pylint: disable=too-few-public-methods

    """
    Abstract class defining a standard signature for all formatters
    """

    @staticmethod
    @abstractmethod
    def export_project(kili, export_params: ExportParams, logger_params: LoggerParams) -> str:
        """
        Export a project to a json.
        Return the name of the exported archive file in the bucket.
        """
