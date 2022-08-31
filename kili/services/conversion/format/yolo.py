"""
Functions to export a project to YOLOv4 or v5 format
"""

import csv
import json
import logging
import os
import shutil
from tempfile import TemporaryDirectory
from typing import Dict, List

import requests

from kili.orm import AnnotationFormat, JobMLTask, JobTool
from kili.services.conversion.format.base import BaseFormatter, ExportParams
from kili.services.conversion.tools import (
    create_readme_kili_file,
    fetch_assets,
    get_endpoint_router_from_services,
    is_asset_served_by_kili,
)
from kili.services.conversion.typing import ExportType, JobCategory, SplitOption
from kili.services.conversion.video import cut_video, get_content_frames_paths

HEADERS = {
    "Authorization": f"X-API-Key: {os.getenv('KILI__API_KEY')}",
    "X-bypass-key": os.getenv("AUTHORIZATION__BYPASS_KEY"),
}


class DownloadError(Exception):
    """
    Exception thrown when the contents cannot be downloaded.
    """


class YoloFormatter(BaseFormatter):
    # pylint: disable=too-few-public-methods

    """
    Formatter to export to YOLOv4 or YOLOv5
    """

    @staticmethod
    def export_project(kili, export_params: ExportParams) -> None:
        """
        Export a project to YOLO v4 or v5 format
        """
        assets = fetch_assets(
            kili,
            project_id=export_params.project_id,
            asset_ids=export_params.assets_ids,
            export_type=export_params.export_type,
            label_type_in=["DEFAULT", "REVIEW"],
        )
        if export_params.split_option == SplitOption.SPLIT_FOLDER:
            return _process_and_save_yolo_pytorch_export_split(
                kili,
                export_params.export_type,
                assets,
                export_params.project_id,
                export_params.project_name,
                export_params.label_format,
                logging,
                export_params.output_file,
            )
        return _process_and_save_yolo_pytorch_export(
            kili,
            export_params.export_type,
            assets,
            export_params.project_id,
            export_params.project_name,
            export_params.label_format,
            logging,
            export_params.output_file,
        )


def _convert_from_kili_to_yolo_format(job_id, label, category_ids: Dict[str, JobCategory]):
    # pylint: disable=too-many-locals
    """
    Extract formatted annotations from labels and save the zip in the buckets.
    """
    if label is None or "jsonResponse" not in label:
        return []
    json_response = label["jsonResponse"]
    if not (job_id in json_response and "annotations" in json_response[job_id]):
        return []
    annotations = json_response[job_id]["annotations"]
    converted_annotations = []
    for annotation in annotations:
        category_idx: JobCategory = category_ids[
            _get_category_full_name(job_id, annotation["categories"][0]["name"])
        ]
        if "boundingPoly" not in annotation:
            continue
        bounding_poly = annotation["boundingPoly"]
        if len(bounding_poly) < 1 or "normalizedVertices" not in bounding_poly[0]:
            continue
        normalized_vertices = bounding_poly[0]["normalizedVertices"]
        x_s = [vertice["x"] for vertice in normalized_vertices]
        y_s = [vertice["y"] for vertice in normalized_vertices]
        x_min, y_min = min(x_s), min(y_s)
        x_max, y_max = max(x_s), max(y_s)
        _x_, _y_ = (x_max + x_min) / 2, (y_max + y_min) / 2
        _w_, _h_ = x_max - x_min, y_max - y_min

        converted_annotations.append((category_idx.id, _x_, _y_, _w_, _h_))
    return converted_annotations


def _get_category_full_name(job_id, category_name):
    """
    Return a full name to identify uniquely a category
    """
    return f"{job_id}__{category_name}"


def _get_categories_by_job(json_interface, ml_task, tool) -> Dict[str, Dict[str, JobCategory]]:
    """
    Return a dictionary of JobCategory instances by category full name and job id.
    """
    categories_by_job: Dict[str, Dict[str, JobCategory]] = {}
    for job_id, job in json_interface.get("jobs", {}).items():
        if job.get("mlTask") != ml_task or tool not in job.get("tools", []) or job.get("isModel"):
            continue
        categories: Dict[str, JobCategory] = {}
        for cat_id, category in enumerate(job.get("content", {}).get("categories", {})):
            categories[_get_category_full_name(job_id, category)] = JobCategory(
                category_name=category, id=cat_id, job_id=job_id
            )
        categories_by_job[job_id] = categories
    return categories_by_job


def _write_jobs_assets_into_split_folders(
    assets, categories_by_job, root_folder, images_folder, project_id, label_format
) -> None:
    """
    Write assets into split folders.
    """
    for job_id, category_ids in categories_by_job.items():

        base_folder = os.path.join(root_folder, project_id, job_id)
        labels_folder = os.path.join(base_folder, "labels")
        os.makedirs(labels_folder)

        _write_class_file(base_folder, category_ids, label_format)
        remote_content = []
        video_metadata = {}

        for asset in assets:
            asset_remote_content, video_filenames = _process_asset_for_job(
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


def _process_and_save_yolo_pytorch_export_split(
    kili,
    export_type: ExportType,
    assets: List[Dict],
    project_id: str,
    project_name: str,
    label_format: AnnotationFormat,
    logger,
    output_file: str,
):
    # pylint: disable=too-many-locals, too-many-arguments
    """
    Save the assets and annotations to a zip file in the Yolo format.
    Split each job in a different folder with its own class file.
    """
    logger.info("Exporting yolo format splitted")
    _ = project_name

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


def _get_merged_categories(json_interface, ml_task, tool) -> Dict[str, JobCategory]:
    cat_number = 0
    merged_categories_id: Dict[str, JobCategory] = {}
    for job_id, job in json_interface.get("jobs", {}).items():
        if job.get("mlTask") != ml_task or tool not in job.get("tools", []) or job.get("isModel"):
            continue
        for category in job.get("content", {}).get("categories", {}):
            merged_categories_id[_get_category_full_name(job_id, category)] = JobCategory(
                category_name=category, id=cat_number, job_id=job_id
            )
            cat_number += 1
    return merged_categories_id


def _process_and_save_yolo_pytorch_export(
    kili,
    export_type: ExportType,
    assets,
    project_id: str,
    project_name: str,
    label_format: AnnotationFormat,
    logger,
    output_file: str,
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

    with TemporaryDirectory() as folder:
        base_folder = os.path.join(folder, project_id)
        images_folder = os.path.join(base_folder, "images")
        labels_folder = os.path.join(base_folder, "labels")
        os.makedirs(images_folder)
        os.makedirs(labels_folder)

        _write_class_file(base_folder, merged_categories_id, label_format)
        remote_content = []
        video_metadata = {}

        for asset in assets:
            asset_remote_content, video_filenames = _process_asset_for_job(
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
            with open(remote_file_path, "w", encoding="utf8") as file:
                writer = csv.writer(file)
                writer.writerow(remote_content_header)
                writer.writerows(remote_content)
        create_readme_kili_file(kili, folder, project_id, label_format, export_type)
        path_folder = os.path.join(folder, project_id)
        path_archive = shutil.make_archive(path_folder, "zip", path_folder)
        shutil.copy(path_archive, output_file)


def _get_frame_annotations(frame, job_ids, category_ids):
    annotations = []
    for job_id in job_ids:
        job_annotations = _convert_from_kili_to_yolo_format(
            job_id, frame["latestLabel"], category_ids
        )
        annotations += job_annotations

    return annotations


def _write_content_frame_to_file(
    url_content_frame: str, images_folder: str, filename: str, asset_id: str
):
    endpoint_router = os.getenv("ENDPOINT__ROUTER")
    if endpoint_router is None:
        raise ValueError("Missing ENDPOINT__ROUTER environment variable")
    endpoint_api_v2 = os.getenv("ENDPOINT__API_V2")
    if endpoint_api_v2 is None:
        raise ValueError("Missing ENDPOINT__API_V2 environment variable")
    endpoint_api_private = os.getenv("ENDPOINT__API_PRIVATE")
    if endpoint_api_private is None:
        raise ValueError("Missing ENDPOINT__API_PRIVATE environment variable")

    url_content_frame = url_content_frame.replace(
        endpoint_router, get_endpoint_router_from_services()
    ).replace(endpoint_api_v2, endpoint_api_private)

    response = requests.get(
        url_content_frame,
        stream=True,
        headers=HEADERS,
        verify=os.getenv("KILI__VERIFY_SSL") != "False",
    )
    if not response.ok:
        # pylint: disable=logging-too-many-args
        raise DownloadError("Error while downloading image {}".format(asset_id))

    with open(os.path.join(images_folder, f"{filename}.jpg"), "wb") as fout:
        for block in response.iter_content(1024):
            if not block:
                break
            fout.write(block)


class LabelFrames:
    """
    Holds asset frames data.
    """

    @staticmethod
    def from_asset(asset, job_ids) -> "LabelFrames":
        """
        Instantiate the label frames from the asset. It handles the case when there are several
        frames by label or a single one.
        """
        frames = {}
        number_of_frames = 0
        is_frame_group = False
        if "jsonResponse" in asset["latestLabel"]:
            number_of_frames = len(asset["latestLabel"]["jsonResponse"])
            for idx in range(number_of_frames):
                if str(idx) in asset["latestLabel"]["jsonResponse"]:
                    is_frame_group = True
                    frame_asset = asset["latestLabel"]["jsonResponse"][str(idx)]
                    for job_id in job_ids:
                        if (
                            job_id in frame_asset
                            and "annotations" in frame_asset[job_id]
                            and frame_asset[job_id]["annotations"]
                        ):
                            frames[idx] = {"latestLabel": {"jsonResponse": frame_asset}}
                            break

        if not frames:
            frames[-1] = asset
        return LabelFrames(frames, number_of_frames, is_frame_group, asset["externalId"])

    def __init__(self, frames, number_frames, is_frame_group, external_id) -> None:
        self.frames = frames
        self.number_frames = number_frames
        self.is_frame_group = is_frame_group
        self.external_id = external_id

    def get_leading_zeros(self) -> int:
        """
        Get leading zeros for file name building
        """
        return len(str(self.number_frames))

    def get_label_filename(self, idx: int) -> str:
        """
        Get label filemame for index
        """
        return f"{self.external_id}_{str(idx + 1).zfill(self.get_leading_zeros())}"


def _write_label_file(labels_folder, filename, annotations):
    with open(os.path.join(labels_folder, f"{filename}.txt"), "wb") as fout:
        for category_idx, _x_, _y_, _w_, _h_ in annotations:
            fout.write(f"{category_idx} {_x_} {_y_} {_w_} {_h_}\n".encode())


def _process_asset_for_job(
    asset, images_folder, labels_folder, category_ids: Dict[str, JobCategory]
):
    # pylint: disable=too-many-locals, too-many-branches
    """
    Process an asset for all job_ids of category_ids
    """
    asset_remote_content = []
    job_ids = set(map(lambda job_category: job_category.job_id, category_ids.values()))

    label_frames = LabelFrames.from_asset(asset, job_ids)

    content_frames = get_content_frames_paths(asset, HEADERS)

    video_filenames = []

    for idx, frame in label_frames.frames.items():
        if label_frames.is_frame_group:
            filename = label_frames.get_label_filename(idx)
            video_filenames.append(filename)
        else:
            filename = asset["externalId"]

        annotations = _get_frame_annotations(frame, job_ids, category_ids)

        if not annotations:
            continue

        _write_label_file(labels_folder, filename, annotations)

        content_frame = content_frames[idx] if content_frames else asset["content"]
        if is_asset_served_by_kili(content_frame):
            if content_frames and not label_frames.is_frame_group:
                try:
                    _write_content_frame_to_file(
                        content_frame, images_folder, filename, asset["id"]
                    )
                except DownloadError as download_error:
                    logging.warning(str(download_error))
        else:
            asset_remote_content.append([asset["externalId"], content_frame, f"{filename}.txt"])

    if (
        not content_frames
        and label_frames.is_frame_group
        and is_asset_served_by_kili(asset["content"])
    ):
        cut_video(
            asset,
            label_frames.frames,
            images_folder,
            asset["externalId"],
            label_frames.get_leading_zeros(),
        )

    return asset_remote_content, video_filenames


def _write_class_file(folder, category_ids: Dict[str, JobCategory], label_format):
    """
    Create a file that contains meta information about the export, depending of Yolo version
    """
    if label_format == AnnotationFormat.YoloV4:
        with open(os.path.join(folder, "classes.txt"), "wb") as fout:
            for job_category in category_ids.values():
                fout.write(f"{job_category.id} {job_category.category_name}\n".encode())
    if label_format == AnnotationFormat.YoloV5:
        with open(os.path.join(folder, "data.yaml"), "wb") as fout:
            categories = ""
            for job_category in category_ids.values():
                categories += f"'{job_category.category_name}', "
            fout.write(f"nc: {len(category_ids.items())}\n".encode())
            fout.write(f"names: [{categories[:-2]}]\n".encode())
