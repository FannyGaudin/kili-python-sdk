"""
Types used by the conversion service
"""
from enum import Enum
from typing import List, NamedTuple, Optional

from pydantic import BaseModel


class LabelFormat(str, Enum):
    """
    Export format (later : will be extended for COCO, Tensorflow, etc...)
    """

    RAW = "RAW"
    SIMPLE = "SIMPLE"
    PASCAL_VOC = "PASCAL_VOC"
    YOLO_V4 = "YOLO_V4"
    YOLO_V5 = "YOLO_V5"


class ExportType(str, Enum):
    """
    Export type (either all labels or latest label)
    """

    LATEST = "LATEST"
    NORMAL = "NORMAL"


class SplitOption(str, Enum):
    """
    Do a separate export for each job, or group all jobs.
    Yolo only.
    """

    MERGED_FOLDER = "MERGED_FOLDER"
    SPLIT_FOLDER = "SPLIT_FOLDER"


class ExportPayload(BaseModel):
    """
    Service payload
    """

    # pylint: disable=too-few-public-methods
    assets_ids: Optional[List[str]] = []
    export_type: Optional[ExportType] = ExportType.LATEST
    email: Optional[str] = ""
    label_format: Optional[LabelFormat] = LabelFormat.RAW
    notification_id: Optional[str] = ""
    project_id: str = ""
    project_name: str = ""
    project_version_id: Optional[str] = ""
    split_option: Optional[SplitOption] = SplitOption.MERGED_FOLDER


class JobCategory(NamedTuple):
    """
    Contains information for a category
    """

    category_name: str
    id: int
    job_id: str
