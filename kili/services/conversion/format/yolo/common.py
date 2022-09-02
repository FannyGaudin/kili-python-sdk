import logging
import os
from typing import Dict, List, Set

import requests

from kili.orm import AnnotationFormat
from kili.services.conversion.tools import (
    get_endpoint_router_from_services,
    is_asset_served_by_kili,
)
from kili.services.conversion.typing import JobCategory, YoloAnnotation
from kili.services.conversion.video import cut_video, get_content_frames_paths

HEADERS = {
    "Authorization": f"X-API-Key: {os.getenv('KILI__API_KEY')}",
    "X-bypass-key": os.getenv("AUTHORIZATION__BYPASS_KEY"),
}


class DownloadError(Exception):
    """
    Exception thrown when the contents cannot be downloaded.
    """


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

    def __init__(
        self, frames: Dict[int, Dict], number_frames: int, is_frame_group: bool, external_id: str
    ) -> None:
        self.frames: Dict[int, Dict] = frames
        self.number_frames: int = number_frames
        self.is_frame_group: bool = is_frame_group
        self.external_id: str = external_id

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


def _convert_from_kili_to_yolo_format(
    job_id: str, label: Dict, category_ids: Dict[str, JobCategory]
) -> List[YoloAnnotation]:
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
            get_category_full_name(job_id, annotation["categories"][0]["name"])
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


def get_category_full_name(job_id: str, category_name: str):
    """
    Return a full name to identify uniquely a category
    """
    return f"{job_id}__{category_name}"


def process_asset_for_job(
    asset: Dict, images_folder: str, labels_folder: str, category_ids: Dict[str, JobCategory]
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


def write_class_file(
    folder: str, category_ids: Dict[str, JobCategory], label_format: AnnotationFormat
):
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


def _get_frame_annotations(
    frame: Dict, job_ids: Set[str], category_ids: Dict[str, JobCategory]
) -> List[YoloAnnotation]:
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
        raise DownloadError(f"Error while downloading image {asset_id}")

    with open(os.path.join(images_folder, f"{filename}.jpg"), "wb") as fout:
        for block in response.iter_content(1024):
            if not block:
                break
            fout.write(block)


def _write_label_file(labels_folder: str, filename: str, annotations: List[YoloAnnotation]):
    with open(os.path.join(labels_folder, f"{filename}.txt"), "wb") as fout:
        for category_idx, _x_, _y_, _w_, _h_ in annotations:
            fout.write(f"{category_idx} {_x_} {_y_} {_w_} {_h_}\n".encode())
