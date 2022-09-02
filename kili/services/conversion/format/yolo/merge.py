"""
Functions to export a project to YOLOv4 or v5 format, with a merged folder layout
"""
import logging
import os
import shutil
from tempfile import TemporaryDirectory
from typing import Dict, List

from kili.orm import AnnotationFormat
from kili.services.conversion.format.yolo.common import (
    get_category_full_name,
    get_project_and_init,
    write_labels_into_single_folder,
)
from kili.services.conversion.tools import create_readme_kili_file
from kili.services.conversion.typing import ExportType, JobCategory


def process_and_save_yolo_pytorch_export_merge(
    kili,
    export_type: ExportType,
    assets: List[Dict],
    project_id: str,
    label_format: AnnotationFormat,
    logger,
    output_filename: str,
    disable_tqdm: bool,
) -> None:
    # pylint: disable=too-many-locals, too-many-arguments
    """
    Save the assets and annotations to a zip file in the Yolo format.
    """
    logger = logging.getLogger("kili.services.conversion.export")
    logger.warning("Exporting to yolo format merged...")

    json_interface, ml_task, tool = get_project_and_init(kili, project_id)
    merged_categories_id = _get_merged_categories(json_interface, ml_task, tool)

    with TemporaryDirectory() as root_folder:
        base_folder = os.path.join(root_folder, project_id)
        images_folder = os.path.join(base_folder, "images")
        labels_folder = os.path.join(base_folder, "labels")
        os.makedirs(images_folder)
        os.makedirs(labels_folder)
        write_labels_into_single_folder(
            assets,
            merged_categories_id,
            labels_folder,
            images_folder,
            base_folder,
            label_format,
            disable_tqdm,
        )
        create_readme_kili_file(kili, root_folder, project_id, label_format, export_type)
        path_folder = os.path.join(root_folder, project_id)
        path_archive = shutil.make_archive(path_folder, "zip", path_folder)
        shutil.copy(path_archive, output_filename)

    logger.warning("Done!")


def _get_merged_categories(json_interface: Dict, ml_task: str, tool: str) -> Dict[str, JobCategory]:
    """
    Return a dictionary of JobCategory instances by category full name.
    """
    cat_number = 0
    merged_categories_id: Dict[str, JobCategory] = {}
    for job_id, job in json_interface.get("jobs", {}).items():
        if job.get("mlTask") != ml_task or tool not in job.get("tools", []) or job.get("isModel"):
            continue
        for category in job.get("content", {}).get("categories", {}):
            merged_categories_id[get_category_full_name(job_id, category)] = JobCategory(
                category_name=category, id=cat_number, job_id=job_id
            )
            cat_number += 1
    return merged_categories_id
