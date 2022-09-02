"""
Functions to export a project to YOLOv4 or v5 format
"""

import logging

from kili.services.conversion.format.base import (
    BaseFormatter,
    ExportParams,
    LoggerParams,
)
from kili.services.conversion.format.yolo.merge import (
    process_and_save_yolo_pytorch_export_merge,
)
from kili.services.conversion.format.yolo.split import (
    process_and_save_yolo_pytorch_export_split,
)
from kili.services.conversion.tools import fetch_assets
from kili.services.conversion.typing import SplitOption


class YoloFormatter(BaseFormatter):
    # pylint: disable=too-few-public-methods

    """
    Formatter to export to YOLOv4 or YOLOv5
    """

    @staticmethod
    def export_project(kili, export_params: ExportParams, logger_params: LoggerParams) -> None:
        """
        Export a project to YOLO v4 or v5 format
        """
        logger = logging.getLogger("kili.services.conversion.export")
        logger.warning("Fetching assets ...")
        assets = fetch_assets(
            kili,
            project_id=export_params.project_id,
            asset_ids=export_params.assets_ids,
            export_type=export_params.export_type,
            label_type_in=["DEFAULT", "REVIEW"],
            disable_tqdm=logger_params.disable_tqdm,
        )
        if export_params.split_option == SplitOption.SPLIT_FOLDER:
            return process_and_save_yolo_pytorch_export_split(
                kili,
                export_params.export_type,
                assets,
                export_params.project_id,
                export_params.label_format,
                logger,
                export_params.output_file,
                logger_params.disable_tqdm,
            )
        return process_and_save_yolo_pytorch_export_merge(
            kili,
            export_params.export_type,
            assets,
            export_params.project_id,
            export_params.label_format,
            logger,
            export_params.output_file,
            logger_params.disable_tqdm,
        )
