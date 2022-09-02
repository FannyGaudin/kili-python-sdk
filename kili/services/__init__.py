"""
Python SDK service layer
"""
from logging import getLogger
from typing import List, Optional

from kili.services.conversion.format.base import ExportParams
from kili.services.conversion.format.yolo import YoloFormatter
from kili.services.conversion.typing import ExportType, LabelFormat, SplitOption


def export_assets(  # pylint: disable=too-many-arguments
    kili,
    asset_ids: Optional[List[str]],
    project_id: str,
    export_type: ExportType,
    label_format: LabelFormat,
    split_option: SplitOption,
    output_file: str,
) -> None:
    """
    Export the selected assets into the required format, and save it into a file archive.
    """
    getLogger("conversion").info("Exporting to %s format", label_format)
    export_params = ExportParams(
        assets_ids=asset_ids,
        project_id=project_id,
        export_type=export_type,
        label_format=label_format.lower(),
        split_option=split_option,
        output_file=output_file,
    )

    # if label_format in [LabelFormat.RAW, LabelFormat.SIMPLE]:
    # return KiliFormatter.export_project(export_params, request_params)
    if label_format in [LabelFormat.YOLO_V4, LabelFormat.YOLO_V5]:
        YoloFormatter.export_project(kili, export_params)
    # if label_format in [LabelFormat.PASCAL_VOC]:
    #     return VocFormatter.export_project(export_params, request_params)
    # raise Exception('Case not handled')
