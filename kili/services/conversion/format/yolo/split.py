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


# Split folders
def process_and_save_yolo_pytorch_export_split(
    kili,
    export_type: ExportType,
    assets: List[Dict],
    project_id: str,
    project_name: str,
    label_format: AnnotationFormat,
    logger,
    output_file: str,
) -> None:
    # pylint: disable=too-many-locals, too-many-arguments
    """
    Save the assets and annotations to a zip file in the Yolo format.
    Split each job in a different folder with its own class file.
    """
    _ = project_name
    logger.info("Exporting yolo format splitted")

    json_interface = kili.projects(
        project_id=project_id, fields=["jsonInterface"], disable_tqdm=True
    )[0]["jsonInterface"]

    ml_task = JobMLTask.ObjectDetection
    tool = JobTool.Rectangle

    categories_by_job = _get_categories_by_job(json_interface, ml_task, tool)

    with TemporaryDirectory() as root_folder:
        images_folder = os.path.join(root_folder, project_id, "images")
        os.makedirs(images_folder)
        _write_jobs_assets_into_split_folders(
            assets, categories_by_job, root_folder, images_folder, project_id, label_format
        )
        create_readme_kili_file(kili, root_folder, project_id, label_format, export_type)
        path_folder = os.path.join(root_folder, project_id)
        path_archive = shutil.make_archive(path_folder, "zip", path_folder)
        shutil.copy(path_archive, output_file)


def _get_categories_by_job(
    json_interface: Dict, ml_task: str, tool: str
) -> Dict[str, Dict[str, JobCategory]]:
    """
    Return a dictionary of JobCategory instances by category full name and job id.
    """
    categories_by_job: Dict[str, Dict[str, JobCategory]] = {}
    for job_id, job in json_interface.get("jobs", {}).items():
        if job.get("mlTask") != ml_task or tool not in job.get("tools", []) or job.get("isModel"):
            continue
        categories: Dict[str, JobCategory] = {}
        for cat_id, category in enumerate(job.get("content", {}).get("categories", {})):
            categories[get_category_full_name(job_id, category)] = JobCategory(
                category_name=category, id=cat_id, job_id=job_id
            )
        categories_by_job[job_id] = categories
    return categories_by_job


def _write_jobs_assets_into_split_folders(
    assets: List[Dict],
    categories_by_job: Dict[str, Dict[str, JobCategory]],
    root_folder: str,
    images_folder: str,
    project_id: str,
    label_format: AnnotationFormat,
) -> None:
    """
    Write assets into split folders.
    """
    for job_id, category_ids in categories_by_job.items():

        base_folder = os.path.join(root_folder, project_id, job_id)
        labels_folder = os.path.join(base_folder, "labels")
        os.makedirs(labels_folder)

        write_class_file(base_folder, category_ids, label_format)
        remote_content = []
        video_metadata = {}

        for asset in assets:
            asset_remote_content, video_filenames = process_asset_for_job(
                asset, images_folder, labels_folder, category_ids
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
            with open(remote_file_path, "w", encoding="utf8") as file:
                writer = csv.writer(file)
                writer.writerow(remote_content_header)
                writer.writerows(remote_content)
