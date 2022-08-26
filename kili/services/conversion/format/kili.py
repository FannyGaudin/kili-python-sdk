"""
Functions to export a project to Kili format
"""

import json
import os
import shutil
from tempfile import TemporaryDirectory

from app.bucket import get_bucket_client, get_bucket_type, upload_file
from app.format.base import BaseFormatter, ExportParams, RequestParams
from app.logger import get_logger
from app.tools import (
    create_readme_kili_file,
    fetch_assets,
    filter_out_autosave_labels,
    get_export_name,
)

from kili.orm import AnnotationFormat


class KiliFormatter(BaseFormatter):
    # pylint: disable=too-few-public-methods
    """
    Formatter to export to Kili format
    """

    @staticmethod
    def export_project(export_params: ExportParams, request_params: RequestParams):
        """
        Export a project to Kili format
        """
        assets = fetch_assets(
            project_id=export_params.project_id,
            assets_ids=export_params.assets_ids,
            export_type=export_params.export_type,
        )
        return _process_and_save_json_export(assets, export_params, request_params.user_email)


def _process_and_save_json_export(assets, export_params, user_email):
    """
    Extract formatted annotations from labels and save the json in the buckets.
    """
    logger = get_logger(user_email)
    logger.info("Exporting kili format")
    clean_assets = _process_assets(assets, export_params.label_format)
    return _save_assets_export(
        clean_assets,
        export_params.project_id,
        export_params.project_name,
        export_params.label_format,
        export_params.export_type,
    )


def _process_assets(assets, label_format):
    """
    Format labels in the requested format, and filter out autosave labels
    """
    assets_in_format = []
    for asset in assets:
        if "labels" in asset:
            labels_of_asset = []
            for label in asset["labels"]:
                clean_label = _format_json_response(label, label_format)
                labels_of_asset.append(clean_label)
            asset["labels"] = labels_of_asset
        if "latestLabel" in asset:
            label = asset["latestLabel"]
            if label is not None:
                clean_label = _format_json_response(label, label_format)
                asset["latestLabel"] = clean_label
        assets_in_format.append(asset)

    clean_assets = filter_out_autosave_labels(assets_in_format)
    return clean_assets


def _format_json_response(label, label_format):
    """
    Format the label JSON response in the requested format
    """
    formatted_json_response = label.json_response(_format=label_format.lower())
    if label_format.lower() == AnnotationFormat.Simple:
        label["jsonResponse"] = formatted_json_response
    else:
        json_response = {}
        for key, value in formatted_json_response.items():
            if key.isdigit():
                json_response[int(key)] = value
                continue
            json_response[key] = value
        label["jsonResponse"] = json_response
    return label


def _save_assets_export(assets, project_id, project_name, label_format, export_type):
    """
    Save the assets to a file and return the link to that file
    """
    base_name = get_export_name(project_name)
    destination_name = f"{base_name}.zip"
    with TemporaryDirectory() as folder:
        path_folder = os.path.join(folder, project_id)
        os.makedirs(path_folder)
        path_json = os.path.join(folder, project_id, f"{base_name}.json")
        create_readme_kili_file(folder, project_id, label_format, export_type)
        bucket_type = get_bucket_type()
        client = get_bucket_client(bucket_type)
        project_json = json.dumps(assets, sort_keys=True, indent=4)
        if project_json is not None:
            with open(path_json, "wb") as output_file:
                output_file.write(project_json.encode("utf-8"))
        shutil.make_archive(path_folder, "zip", path_folder)
        upload_file(
            client,
            os.getenv("AIRFLOW__EXPORT_BUCKET"),
            f"{path_folder}.zip",
            destination_name,
            bucket_type,
        )
    return destination_name
