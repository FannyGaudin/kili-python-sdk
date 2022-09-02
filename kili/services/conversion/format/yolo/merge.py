"""
Functions to export a project to YOLOv4 or v5 format, with a merged folder layout
"""

import csv
import json
import os
import shutil
from tempfile import TemporaryDirectory
from typing import Dict, List

from kili.orm import AnnotationFormat, JobMLTask, JobTool
from kili.services.conversion.format.yolo.common import (
    get_category_full_name,
    process_asset_for_job,
    write_class_file,
)
from kili.services.conversion.tools import create_readme_kili_file
from kili.services.conversion.typing import ExportType, JobCategory


def process_and_save_yolo_pytorch_export_merge(
    kili,
    export_type: ExportType,
    assets: List[Dict],
    project_id: str,
    project_name: str,
    label_format: AnnotationFormat,
    logger,
    output_filename: str,
) -> None:
    # pylint: disable=too-many-locals, too-many-arguments
    """
    Save the assets and annotations to a zip file in the Yolo format.
    """
    _ = project_name
    logger.info("Exporting yolo format merged")

    json_interface = kili.projects(project_id=project_id, fields=["jsonInterface"])[0][
        "jsonInterface"
    ]
    ml_task = JobMLTask.ObjectDetection
    tool = JobTool.Rectangle

    merged_categories_id = _get_merged_categories(json_interface, ml_task, tool)

    with TemporaryDirectory() as root_folder:
        base_folder = os.path.join(root_folder, project_id)
        images_folder = os.path.join(base_folder, "images")
        labels_folder = os.path.join(base_folder, "labels")
        os.makedirs(images_folder)
        os.makedirs(labels_folder)

        write_class_file(base_folder, merged_categories_id, label_format)
        remote_content = []
        video_metadata = {}

        for asset in assets:
            asset_remote_content, video_filenames = process_asset_for_job(
                asset, images_folder, labels_folder, merged_categories_id
            )
            if video_filenames:
                video_metadata[asset["externalId"]] = video_filenames
            remote_content.extend(asset_remote_content)

        if video_metadata:
            video_metadata_json = json.dumps(video_metadata, sort_keys=True, indent=4)
            if video_metadata_json is not None:
                meta_json_path = os.path.join(base_folder, "video_meta.json")
                with open(meta_json_path, "wb") as output_file:
                    output_file.write(video_metadata_json.encode("utf-8"))

        if len(remote_content) > 0:
            remote_content_header = ["external id", "url", "label file"]
            remote_file_path = os.path.join(images_folder, "remote_assets.csv")
            with open(remote_file_path, "w", encoding="utf8") as fout:
                writer = csv.writer(fout)
                writer.writerow(remote_content_header)
                writer.writerows(remote_content)
        create_readme_kili_file(kili, root_folder, project_id, label_format, export_type)
        path_folder = os.path.join(root_folder, project_id)
        path_archive = shutil.make_archive(path_folder, "zip", path_folder)
        shutil.copy(path_archive, output_filename)


def _get_merged_categories(json_interface: Dict, ml_task: str, tool: str) -> Dict[str, JobCategory]:
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
