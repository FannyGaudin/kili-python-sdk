"""
Functions to export a project to YOLOv4 or v5 format
"""

import logging

from kili.services.conversion.format.base import (
    BaseFormatter,
    ContentRepositoryParams,
    ExportParams,
    LoggerParams,
)
from kili.services.conversion.format.yolo.merge import YoloMergeExporter
from kili.services.conversion.format.yolo.split import YoloSplitExporter
from kili.services.conversion.logger import AbstractLogger
from kili.services.conversion.repository import (
    AbstractContentRepository,
    SDKContentRepository,
)
from kili.services.conversion.tools import fetch_assets
from kili.services.conversion.typing import SplitOption


class YoloFormatter(BaseFormatter):
    # pylint: disable=too-few-public-methods

    """
    Formatter to export to YOLOv4 or YOLOv5
    """

    @staticmethod
    @inject
    def export_project(
        kili,
        export_params: ExportParams,
        logger: AbstractLogger,
        content_repository: AbstractContentRepository,
    ) -> None:
        """
        Export a project to YOLO v4 or v5 format
        """
        logger.logger.warning("Fetching assets ...")
        assets = fetch_assets(
            kili,
            project_id=export_params.project_id,
            asset_ids=export_params.assets_ids,
            export_type=export_params.export_type,
            label_type_in=["DEFAULT", "REVIEW"],
            disable_tqdm=logger.disable_tqdm,
        )

        if export_params.split_option == SplitOption.SPLIT_FOLDER:

            return YoloSplitExporter(
                export_params.project_id,
                export_params.export_type,
                export_params.label_format,
                kili,
                logger,
                content_repository,
            ).process_and_save(assets, export_params.output_file)

        return YoloMergeExporter(
            export_params.project_id,
            export_params.export_type,
            export_params.label_format,
            kili,
            logger,
            content_repository,
        ).process_and_save(assets, export_params.output_file)
