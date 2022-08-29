"""
Set of common functions used by different export formats
"""
import os
import re
from datetime import datetime
from typing import Iterator

from kili.orm import AnnotationFormat

DEFAULT_FIELDS = [  # TODO: check if this is still relevant
    "id",
    "content",
    "externalId",
    "jsonMetadata",
    "labels.author.id",
    "labels.author.email",
    "labels.author.firstname",
    "labels.author.lastname",
    "labels.jsonResponse",
    "labels.createdAt",
    "labels.isLatestLabelForUser",
    "labels.labelType",
    "labels.modelName",
]
LATEST_LABEL_FIELDS = [
    "id",
    "content",
    "externalId",
    "jsonContent",
    "jsonMetadata",
    "latestLabel.author.id",
    "latestLabel.author.email",
    "latestLabel.jsonResponse",
    "latestLabel.author.firstname",
    "latestLabel.author.lastname",
    "latestLabel.createdAt",
    "latestLabel.isLatestLabelForUser",
    "latestLabel.labelType",
    "latestLabel.modelName",
]
KILI_FILES = (
    f'{os.getenv("ENDPOINT__ROUTER")}'
    f'{os.getenv("ENDPOINT__API_V2")}{os.getenv("ENDPOINT__FILES")}'
)


def attach_name_to_assets_labels_author(assets, export_type):
    """
    Adds `name` field for author, by concatenating his/her first and last name
    """
    for asset in assets:
        if export_type.lower() == AnnotationFormat.Latest.lower():
            latest_label = asset["latestLabel"]
            if latest_label:
                firstname = latest_label["author"]["firstname"]
                lastname = latest_label["author"]["lastname"]
                latest_label["author"]["name"] = f"{firstname} {lastname}"
            continue
        for label in asset.get("labels", []):
            firstname = label["author"]["firstname"]
            lastname = label["author"]["lastname"]
            label["author"]["name"] = f"{firstname} {lastname}"


def create_readme_kili_file(kili, folder, project_id, label_format, export_type):
    """
    Create a README.kili.txt file to give information about exported labels
    """
    readme_file_name = os.path.join(folder, project_id, "README.kili.txt")
    project_info = kili.projects(project_id=project_id, fields=["title", "id", "description"])[0]
    with open(readme_file_name, "wb") as fout:
        fout.write("Exported Labels from KILI\n=========================\n\n".encode())
        fout.write(f"- Project name: {project_info['title']}\n".encode())
        fout.write(f"- Project identifier: {project_id}\n".encode())
        fout.write(f"- Project description: {project_info['description']}\n".encode())
        fout.write(f'- Export date: {datetime.now().strftime("%Y%m%d-%H%M%S")}\n'.encode())
        fout.write(f"- Exported format: {label_format}\n".encode())
        fout.write(f"- Exported labels: {export_type}\n".encode())


def fetch_assets(kili, project_id, asset_ids, export_type, label_type_in=None):
    """
    Fetches assets where ID are in asset_ids if the list has more than one element,
    else all the assets of the project

    Parameters
    ----------
    - project_id: project id
    - assets_ids: list of asset IDs
    - export_type: type of export (latest label or all labels)
    - label_type_in: types of label to fetch (default, reviewed, ...)
    """
    fields = get_fields_to_fetch(export_type)
    assets = None
    if asset_ids is not None and len(asset_ids) > 0:
        assets = kili.assets(
            asset_id_in=asset_ids,
            project_id=project_id,
            fields=fields,
            label_type_in=label_type_in,
        )
    else:
        assets = kili.assets(project_id=project_id, fields=fields, label_type_in=label_type_in)
    attach_name_to_assets_labels_author(assets, export_type)
    return assets


def filter_out_autosave_labels(assets):
    """
    Removes AUTOSAVE labels from exports

    Parameters
    ----------
    - assets: list of assets
    """
    clean_assets = []
    for asset in assets:
        labels = asset.get("labels", [])
        clean_labels = list(filter(lambda label: label["labelType"] != "AUTOSAVE", labels))
        if clean_labels:
            asset["labels"] = clean_labels
        clean_assets.append(asset)
    return clean_assets


def generates_jobs_list(kili, project_id, ml_task, tool) -> Iterator[str]:
    """
    Fetches the interface of the project and returns the list of jobs
    matching the ml_task and tool type

    Parameters
    ----------
    - project_id: project id
    - ml_task: ML Task (eg. OBJECT_DETECTION)
    - tool: type of the tool (eg. rectangle)
    """
    projects = kili.projects(project_id=project_id, fields=["id", "jsonInterface"])
    assert len(projects) == 1
    json_interface = projects[0]["jsonInterface"]
    for job_id, job in json_interface.get("jobs", {}).items():
        if job.get("mlTask") == ml_task and tool in job.get("tools", []):
            yield job_id


def get_export_name(project_name):
    """
    Return the base name of exported file
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    cleaned_project_name = re.sub(r"[\<\>\:\"\/\\\|\?\*]", "-", project_name)
    return f"kili-label-export-{cleaned_project_name}-{timestamp}"


def get_fields_to_fetch(export_type):
    """
    Returns the fields to fetch depending on the export type
    """
    if export_type.lower() == AnnotationFormat.Latest.lower():
        return LATEST_LABEL_FIELDS
    return DEFAULT_FIELDS


def is_asset_served_by_kili(url):
    """
    Return a boolean defining if the asset is served by Kili or not
    """
    return url.startswith(KILI_FILES)


def get_endpoint_router_from_services():
    """
    Return the enpoint of the router which is changed for development env
    since most devs use export inside docker and router outside
    """
    endpoint = os.getenv("ENDPOINT__ROUTER_URL_FROM_SERVICE")

    if os.getenv("ENVIRONMENT") == "development":
        endpoint = endpoint.replace("localhost", "host.docker.internal")

    return endpoint
