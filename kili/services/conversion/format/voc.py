"""
Functions to export a project to Pascal VOC format
"""

import csv
import json
import logging
import os
import shutil
import xml.etree.ElementTree as ET
from tempfile import NamedTemporaryFile, TemporaryDirectory
from xml.dom import minidom

import ffmpeg
import numpy as np
import requests
from app.bucket import get_bucket_client, get_bucket_type, upload_file
from app.format.base import BaseFormatter, ExportParams, RequestParams
from app.logger import get_logger
from app.tools import (
    create_readme_kili_file,
    fetch_assets,
    get_endpoint_router_from_services,
    get_export_name,
    is_asset_served_by_kili,
)
from app.types import ExportType
from app.video import cut_video, get_content_frames
from PIL import Image

from kili.orm import AnnotationFormat


class VocFormatter(BaseFormatter):
    # pylint: disable=too-few-public-methods

    """
    Formatter to export to Pascal VOC
    """

    @staticmethod
    def export_project(export_params: ExportParams, request_params: RequestParams):
        """
        Export a project to Pascal VOC format
        """
        assets = fetch_assets(
            project_id=export_params.project_id,
            assets_ids=export_params.assets_ids,
            export_type=export_params.export_type,
            label_type_in=["DEFAULT", "REVIEW"],
        )
        return _process_and_save_pascal_voc_export(
            export_params.export_type,
            assets,
            export_params.project_id,
            export_params.project_name,
            export_params.label_format,
            request_params.user_email,
        )


def _parse_annotations(response, xml_label, width, height):
    # pylint: disable=too-many-locals
    for _, job_response in response.items():
        if "annotations" in job_response:
            annotations = job_response["annotations"]
            for annotation in annotations:
                vertices = annotation["boundingPoly"][0]["normalizedVertices"]
                categories = annotation["categories"]
                for category in categories:
                    annotation_category = ET.SubElement(xml_label, "object")
                    name = ET.SubElement(annotation_category, "name")
                    name.text = category["name"]
                    pose = ET.SubElement(annotation_category, "pose")
                    pose.text = "Unspecified"
                    truncated = ET.SubElement(annotation_category, "truncated")
                    truncated.text = "0"
                    difficult = ET.SubElement(annotation_category, "difficult")
                    difficult.text = "0"
                    occluded = ET.SubElement(annotation_category, "occluded")
                    occluded.text = "0"
                    bndbox = ET.SubElement(annotation_category, "bndbox")
                    x_vertices = [int(np.round(v["x"] * width)) for v in vertices]
                    y_vertices = [int(np.round(v["y"] * height)) for v in vertices]
                    xmin = ET.SubElement(bndbox, "xmin")
                    xmin.text = str(min(x_vertices))
                    xmax = ET.SubElement(bndbox, "xmax")
                    xmax.text = str(max(x_vertices))
                    ymin = ET.SubElement(bndbox, "ymin")
                    ymin.text = str(min(y_vertices))
                    ymax = ET.SubElement(bndbox, "ymax")
                    ymax.text = str(max(y_vertices))


def _provide_voc_headers(xml_label, width, height, parameters):
    folder = ET.SubElement(xml_label, "folder")
    folder.text = parameters.get("folder", "")

    filename = ET.SubElement(xml_label, "filename")
    filename.text = parameters.get("filename", "")

    path = ET.SubElement(xml_label, "path")
    path.text = parameters.get("path", "")

    source = ET.SubElement(xml_label, "source")
    database = ET.SubElement(source, "database")
    database.text = "Kili Technology"

    size = ET.SubElement(xml_label, "size")
    width_xml = ET.SubElement(size, "width")
    width_xml.text = str(width)
    height_xml = ET.SubElement(size, "height")
    height_xml.text = str(height)
    depth = ET.SubElement(size, "depth")
    depth.text = parameters.get("depth", "3")

    segmented = ET.SubElement(xml_label, "segmented")
    segmented.text = 0


def _convert_from_kili_to_voc_format(response, width, height, parameters):
    xml_label = ET.Element("annotation")

    _provide_voc_headers(xml_label, width, height, parameters=parameters)

    _parse_annotations(response, xml_label, width, height)

    xmlstr = minidom.parseString(ET.tostring(xml_label)).toprettyxml(indent="   ")

    return xmlstr


def get_asset_dimensions(url, is_frame):
    """
    Download an asset and get width and height
    """
    content = url.replace(os.getenv("ENDPOINT__ROUTER"), get_endpoint_router_from_services())

    headers = None
    if is_asset_served_by_kili(url):
        headers = {
            "Authorization": f"X-API-Key: {os.getenv('KILI__API_KEY')}",
            "X-bypass-key": os.getenv("AUTHORIZATION__BYPASS_KEY"),
        }
    response = requests.get(content, stream=True, headers=headers)
    with NamedTemporaryFile() as downloaded_file:
        with open(downloaded_file.name, "wb") as fout:
            for block in response.iter_content(1024):
                if not block:
                    break
                fout.write(block)
        if is_frame is True:
            probe = ffmpeg.probe(downloaded_file.name)
            video_info = next(s for s in probe["streams"] if s["codec_type"] == "video")
            width = video_info["width"]
            height = video_info["height"]
        else:
            image = Image.open(downloaded_file.name)
            width, height = image.size

    return width, height


def _process_asset_for_job(asset, images_folder, labels_folder):
    # pylint: disable=too-many-locals, too-many-branches
    """
    Process an asset
    """
    frames = {}
    is_frame = False
    asset_remote_content = []

    if "jsonResponse" in asset["latestLabel"]:
        number_of_frames = len(asset["latestLabel"]["jsonResponse"])
        leading_zeros = len(str(number_of_frames))
        for idx in range(number_of_frames):
            if str(idx) in asset["latestLabel"]["jsonResponse"]:
                is_frame = True
                frame_asset = asset["latestLabel"]["jsonResponse"][str(idx)]
                frames[idx] = {"latestLabel": {"jsonResponse": frame_asset}}

    if not frames:
        frames[-1] = asset

    headers = {
        "Authorization": f"X-API-Key: {os.getenv('KILI__API_KEY')}",
        "X-bypass-key": os.getenv("AUTHORIZATION__BYPASS_KEY"),
    }

    content_frames = get_content_frames(asset, headers)
    video_filenames = []
    content_frame = content_frames[idx] if content_frames else asset["content"]

    width, height = get_asset_dimensions(content_frame, is_frame)
    for idx, frame in frames.items():
        if is_frame:
            filename = f"{asset['externalId']}_{str(idx + 1).zfill(leading_zeros)}"
            video_filenames.append(filename)
        else:
            filename = asset["externalId"]
        latest_label = frame["latestLabel"]
        json_response = latest_label["jsonResponse"]

        parameters = {"filename": f"{filename}.xml"}
        annotations = _convert_from_kili_to_voc_format(json_response, width, height, parameters)

        if not annotations:
            continue

        with open(os.path.join(labels_folder, f"{filename}.xml"), "wb") as fout:
            fout.write(f"{annotations}\n".encode())

        asset_remote_content.append([asset["externalId"], content_frame, f"{filename}.xml"])

        if is_asset_served_by_kili(content_frame):
            if content_frames or not is_frame:
                content_frame = content_frame.replace(
                    os.getenv("ENDPOINT__ROUTER"), get_endpoint_router_from_services()
                ).replace(os.getenv("ENDPOINT__API_V2"), os.getenv("ENDPOINT__API_PRIVATE"))

                response = requests.get(
                    content_frame,
                    stream=True,
                    headers=headers,
                    verify=os.getenv("KILI__VERIFY_SSL") != "False",
                )
                if not response.ok:
                    # pylint: disable=logging-too-many-args
                    logging.warning("Error while downloading image %s", asset["id"])

                    continue

                with open(os.path.join(images_folder, f"{filename}.jpg"), "wb") as fout:
                    for block in response.iter_content(1024):
                        if not block:
                            break
                        fout.write(block)

    if not content_frames and is_frame and is_asset_served_by_kili(asset["content"]):
        cut_video(asset, frames, images_folder, asset["externalId"], leading_zeros)

    return asset_remote_content, video_filenames


def _process_and_save_pascal_voc_export(
    export_type: ExportType,
    assets,
    project_id: str,
    project_name: str,
    label_format: AnnotationFormat,
    user_email: str,
):
    # pylint: disable=too-many-locals, too-many-arguments
    """
    Save the assets and annotations to a zip file in the Pascal VOC format.
    """
    logger = get_logger(user_email)
    logger.info("Exporting VOC format")

    base_name = get_export_name(project_name)
    destination_name = f"{base_name}.zip"
    with TemporaryDirectory() as folder:
        base_folder = os.path.join(folder, project_id)
        images_folder = os.path.join(base_folder, "images")
        labels_folder = os.path.join(base_folder, "labels")
        os.makedirs(images_folder)
        os.makedirs(labels_folder)

        remote_content = []
        video_metadata = {}

        for asset in assets:
            asset_remote_content, video_filenames = _process_asset_for_job(
                asset, images_folder, labels_folder
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
        create_readme_kili_file(folder, project_id, label_format, export_type)
        path_folder = os.path.join(folder, project_id)
        shutil.make_archive(path_folder, "zip", path_folder)
        bucket_type = get_bucket_type()
        client = get_bucket_client(bucket_type)
        upload_file(
            client,
            os.getenv("AIRFLOW__EXPORT_BUCKET"),
            f"{path_folder}.zip",
            destination_name,
            bucket_type,
        )
    return destination_name
