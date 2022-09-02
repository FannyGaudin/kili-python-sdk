"""
Base class for all formatters
"""

from abc import ABC, abstractmethod
from typing import List, NamedTuple

from kili.orm import AnnotationFormat
from kili.services.conversion.typing import ExportType, SplitOption


class ExportParams(NamedTuple):
    """
    Contains all parameters that change the result of the export
    """

    assets_ids: List[str]
    export_type: ExportType
    project_id: str
    project_name: str
    label_format: AnnotationFormat
    split_option: SplitOption


class RequestParams(NamedTuple):
    """
    Contains all parameters not directly linked to export
    """

    user_email: str


class BaseFormatter(ABC):
    # pylint: disable=too-few-public-methods

    """
    Abstract class defining a standard signature for all formatters
    """

    @staticmethod
    @abstractmethod
    def export_project(kili: "Kili", export_params: ExportParams) -> str:
        """
        Export a project to a json.
        Return the name of the exported archive file in the bucket.
        """
