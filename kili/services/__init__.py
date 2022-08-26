import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List

from typing_extensions import Literal

from kili.services.conversion.format.base import ExportParams
from kili.services.conversion.format.yolo import YoloFormatter
from kili.services.conversion.typing import ExportType, LabelFormat, SplitOption

# OutputFormat = Literal['RAW', 'SIMPLE', 'PASCAL_VOC', 'YOLO_V4', 'YOLO_V5']
# ExportType = Literal["LATEST", "NORMAL"]
# SplitOption = Literal['MERGED_FOLDER', 'SPLIT_FOLDER']

# assets_ids=payload.assets_ids,
# project_id=payload.project_id,
# project_name=payload.project_name,
# export_type=payload.export_type,
# label_format=label_format.lower(),
# split_option=payload.split_option,


def convert_assets(
    kili,
    asset_ids: List[str],
    project_id: str,
    project_name: str,
    export_type: ExportType,
    label_format: LabelFormat,
    split_option: SplitOption,
    output_file: str,
) -> str:
    """ """
    export_params = ExportParams(
        assets_ids=asset_ids,
        project_id=project_id,
        project_name=project_name,
        export_type=export_type,
        label_format=label_format.lower(),
        split_option=split_option,
    )

    # if label_format in [LabelFormat.RAW, LabelFormat.SIMPLE]:
    # return KiliFormatter.export_project(export_params, request_params)
    if label_format in [LabelFormat.YOLO_V4, LabelFormat.YOLO_V5]:
        return YoloFormatter.export_project(kili, export_params)
    # if label_format in [LabelFormat.PASCAL_VOC]:
    #     return VocFormatter.export_project(export_params, request_params)
    # raise Exception('Case not handled')
