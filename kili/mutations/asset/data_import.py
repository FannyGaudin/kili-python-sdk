import mimetypes
import os
import time
from enum import Enum
from json import dumps
from pathlib import Path
from typing import List
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import requests
from tqdm import tqdm

from kili.constants import LARGE_IMAGE_THRESHOLD_SIZE, NO_ACCESS_RIGHT, THROTTLING_DELAY
from kili.helpers import check_file_mime_type, format_result, is_url
from kili.mutations.asset.helpers import add_video_parameters
from kili.mutations.asset.queries import (
    GQL_APPEND_MANY_FRAMES_TO_DATASET,
    GQL_APPEND_MANY_TO_DATASET,
)
from kili.orm import Asset
from kili.queries.asset.queries import GQL_CREATE_UPLOAD_BUCKET_SIGNED_URLS
from kili.queries.project import QueriesProject
from kili.utils.pagination import batch_iterator_builder


class DataImportProcess(Enum):
    Synchronous = "synchronous"
    Asynchronous = "asynchronous"


class DataLocation(Enum):
    Local = "local"
    Hosted = "hosted"
    RawText = "raw_text"


class AsynchronousUploadType(Enum):
    GeoSatellite = "GEO_SATELLITE"
    Video = "VIDEO"


AZURE_STRING = "blob.core.windows.net"
GCP_STRING = "storage.googleapis.com"
GCP_STRING_PUBLIC = "storage.cloud.google.com"


class DataImporter:
    def __init__(
        self,
        content_array,
        json_content_array,
        external_id_array,
        is_honeypot_array,
        status_array,
        json_metadata_array,
        project_id,
        auth,
    ):
        self.project_id = project_id
        self.auth = auth
        projects = QueriesProject(auth).projects(project_id, disable_tqdm=True)
        assert len(projects) == 1, NO_ACCESS_RIGHT
        self.input_type = projects[0]["inputType"]

        self.is_uploading_content = content_array is not None
        self.is_uploading_json_content = json_content_array is not None
        if not self.is_uploading_content and not self.is_uploading_json_content:
            raise ValueError("Variables content_array and json_content_array cannot be both None.")

        size_of_parameter_arrays = [
            len(array)
            for array in [
                content_array,
                json_content_array,
                external_id_array,
                is_honeypot_array,
                status_array,
                json_metadata_array,
            ]
            if array is not None
        ]
        if len(set(size_of_parameter_arrays)) != 1:
            raise ValueError("All arrays in the parameters should have the same size")
        nb_data = size_of_parameter_arrays[0]

        self.content_array = content_array or [""] * nb_data
        self.json_content_array = json_content_array or [""] * nb_data
        self.external_id_array = external_id_array or [uuid4().hex for _ in range(nb_data)]
        self.is_honeypot_array = is_honeypot_array or [False] * nb_data
        self.status_array = status_array or ["TODO"] * nb_data
        self.json_metadata_array = json_metadata_array or [{}] * nb_data

    def process_json_content(self):
        """
        Process the array of json_contents
        """
        if self.is_uploading_json_content and self.input_type not in ("FRAME", "VIDEO"):
            self.json_content_array = list(map(dumps, self.json_content_array))

    def process_metadata(self):
        """
        Process the metadata of each asset
        """
        if self.input_type in ("FRAME", "VIDEO"):
            should_use_native_video = not self.is_uploading_json_content
            self.json_metadata_array = [
                add_video_parameters(json_metadata, should_use_native_video)
                for json_metadata in self.json_metadata_array
            ]
        self.json_metadata_array = list(map(dumps, self.json_metadata_array))

    def split_data_index_by_upload_process(self) -> dict:
        indexes_upload_process = {
            (DataImportProcess.Synchronous, DataLocation.Local): [],
            (DataImportProcess.Synchronous, DataLocation.Hosted): [],
            (DataImportProcess.Asynchronous, DataLocation.Local): [],
            (DataImportProcess.Asynchronous, DataLocation.Hosted): [],
            (DataImportProcess.Synchronous, DataLocation.RawText): [],
        }
        if not self.is_uploading_json_content:
            for index, content in enumerate(self.content_array):
                is_url_content = is_url(content)
                is_local_file = Path(content).is_file()
                is_raw_text = self.input_type == "TEXT" and not is_url_content and not is_local_file

                is_video_into_frames = (
                    self.input_type
                    in (
                        "FRAME",
                        "VIDEO",
                    )
                    and not self.json_metadata_array[index]["processingParameters"][
                        "shouldUseNativeVideo"
                    ]
                )
                if is_url_content:
                    if is_video_into_frames:
                        indexes_upload_process[
                            DataImportProcess.Asynchronous, DataLocation.Hosted
                        ].append(index)
                    else:
                        indexes_upload_process[
                            DataImportProcess.Synchronous, DataLocation.Hosted
                        ].append(index)
                elif is_local_file:
                    check_file_mime_type(content, self.input_type)
                    is_image_tiff = (
                        self.input_type == "IMAGE"
                        and mimetypes.guess_type(content)[0] == "image/tiff"
                    )
                    is_large_image = (
                        self.input_type == "IMAGE"
                        and os.path.getsize(content) >= LARGE_IMAGE_THRESHOLD_SIZE
                    )
                    if is_image_tiff or is_large_image or is_video_into_frames:
                        indexes_upload_process[
                            DataImportProcess.Asynchronous, DataLocation.Local
                        ].append(index)
                    else:
                        indexes_upload_process[
                            DataImportProcess.Synchronous, DataLocation.Local
                        ].append(index)
                elif is_raw_text:
                    indexes_upload_process[
                        DataImportProcess.Synchronous, DataLocation.RawText
                    ].append(index)
                else:
                    raise ValueError(
                        f"data of index {index}, of content: {content} "
                        "is not found on the local machine"
                    )
        else:
            for index, json_content in enumerate(self.json_content_array):
                is_video_from_frames = self.input_type in ("FRAME", "VIDEO")
                is_rich_text = self.input_type == "TEXT"
                if is_video_from_frames:
                    are_local_frames = all([Path(element).is_file() for element in json_content])
                    are_hosted_frames = all([is_url(element) for element in json_content])
                    if are_local_frames:
                        indexes_upload_process[
                            DataImportProcess.Synchronous, DataLocation.Local
                        ].append(index)
                    elif are_hosted_frames:
                        indexes_upload_process[
                            DataImportProcess.Synchronous, DataLocation.Hosted
                        ].append(index)
                    else:
                        raise ValueError(
                            f"Frames in the json_content of index {index} should either be"
                            "all paths towards local files or all urls towards hosted files"
                        )
                elif is_rich_text:
                    indexes_upload_process[
                        DataImportProcess.Synchronous, DataLocation.RawText
                    ].append(index)
                else:
                    raise ValueError(f"Wrong input for json_content at index {index}")

        return indexes_upload_process

    def import_data_from_paginated_calls(self):
        indexes_upload_process = self.split_data_index_by_upload_process()
        print(indexes_upload_process)
        for (upload_process, data_location), index_list in indexes_upload_process.items():
            if len(index_list) == 0:
                continue

            with tqdm(total=len(index_list)) as pbar:
                for batch_indexes in list(batch_iterator_builder(index_list, batch_size=10)):
                    batch = BatchDataImporter(self, batch_indexes, upload_process, data_location)
                    if batch.data_location == DataLocation.Local:
                        batch.import_local_files_to_bucket()
                    if batch.data_location == DataLocation.RawText:
                        batch.import_raw_text_to_bucket()
                    result = batch.import_data_to_kili()
                    pbar.update(len(batch_indexes))
        return format_result("data", result, Asset)


def upload_data_via_REST(signed_urls, data_array: List[str], content_type_array: List[str]):
    """upload data in buckets' signed URL via REST
    Args:
        signed_urls: Bucket signed URLs to upload local files to
        path_array: a list of file paths, json or text to upload
        content_type: mimetype of the data. It will be infered if not given
    """
    responses = []
    print("upload_data_via_REST", data_array, content_type_array)
    for index, data in enumerate(data_array):
        content_type = content_type_array[index]
        headers = {"Content-type": content_type}
        url_with_id = signed_urls[index]
        url_to_use_for_upload = url_with_id.split("&id=")[0]
        if "blob.core.windows.net" in url_to_use_for_upload:
            headers["x-ms-blob-type"] = "BlockBlob"

        response = requests.put(url_to_use_for_upload, data=data, headers=headers)
        print(response.status_code)
        if response.status_code >= 300:
            responses.append("")
            continue
        responses.append(url_with_id)
    return responses


def getBasePathFromUrl(url: str):
    if AZURE_STRING in url:
        return url.split("?")[0]
    if GCP_STRING in url:
        url_path = urlparse(url).path
        return f"https://{GCP_STRING_PUBLIC}{url_path}"
    return url


def cleanSignedUrl(url: str):
    url_query = urlparse(url).query
    id = parse_qs(url_query)["id"][0]
    baseUrl = getBasePathFromUrl(url)
    return f"{baseUrl}?id={id}"


class BatchDataImporter:
    def __init__(
        self,
        data_importer: DataImporter,
        batch_indexes: List[int],
        upload_process: DataImportProcess,
        data_location: DataLocation,
    ):
        self.project_id = data_importer.project_id
        self.size_batch = len(batch_indexes)
        self.input_type = data_importer.input_type
        self.is_uploading_json_content = data_importer.is_uploading_json_content
        self.auth = data_importer.auth
        self.upload_process = (upload_process,)
        self.data_location = data_location
        self.request = (
            GQL_APPEND_MANY_FRAMES_TO_DATASET
            if upload_process == DataImportProcess.Asynchronous
            else GQL_APPEND_MANY_TO_DATASET
        )
        print(
            data_importer.content_array,
            data_importer.external_id_array,
            data_importer.is_honeypot_array,
            data_importer.json_content_array,
            data_importer.json_metadata_array,
        )
        (
            self.content_array_batch,
            self.external_id_array_batch,
            self.is_honeypot_array_batch,
            self.status_array_batch,
            self.json_content_array_batch,
            self.json_metadata_array_batch,
        ) = [
            list(map(array.__getitem__, batch_indexes))
            for array in [
                data_importer.content_array,
                data_importer.external_id_array,
                data_importer.is_honeypot_array,
                data_importer.status_array,
                data_importer.json_content_array,
                data_importer.json_metadata_array,
            ]
        ]

    def upload_content_on_bucket(self):
        create_signed_urls_payload = {
            "projectID": self.project_id,
            "size": self.size_batch,
        }
        mutation_start = time.time()
        urls_response = self.auth.client.execute(
            GQL_CREATE_UPLOAD_BUCKET_SIGNED_URLS, create_signed_urls_payload
        )
        mutation_time = time.time() - mutation_start
        if mutation_time < THROTTLING_DELAY:
            time.sleep(THROTTLING_DELAY - mutation_time)
        signed_urls = urls_response["data"]["urls"]
        if self.data_location == DataLocation.RawText:
            data_array = self.content_array_batch
            content_type_array = ["text/plain"] * self.size_batch
        else:
            data_array = [open(path, "rb") for path in self.content_array_batch]
            content_type_array = [
                mimetypes.guess_type(path)[0] for path in self.content_array_batch
            ]
        urls_with_uploaded_data = upload_data_via_REST(signed_urls, data_array, content_type_array)
        return urls_with_uploaded_data

    def upload_rich_text_on_bucket(self):
        create_signed_urls_payload = {
            "projectID": self.project_id,
            "size": self.size_batch,
        }
        mutation_start = time.time()
        urls_response = self.auth.client.execute(
            GQL_CREATE_UPLOAD_BUCKET_SIGNED_URLS, create_signed_urls_payload
        )
        mutation_time = time.time() - mutation_start
        if mutation_time < THROTTLING_DELAY:
            time.sleep(THROTTLING_DELAY - mutation_time)
        signed_urls = urls_response["data"]["urls"]
        return upload_data_via_REST(
            signed_urls,
            self.json_content_array_batch,
            content_type_array=["text/plain"] * self.size_batch,
        )

    def upload_frames_on_bucket(self):
        nb_signed_urls_batch = self.size_batch + sum(
            [len(json_content) for json_content in self.json_content_array_batch]
        )
        create_signed_urls_payload = {
            "projectID": self.project_id,
            "size": nb_signed_urls_batch,
        }
        mutation_start = time.time()
        urls_response = self.auth.client.execute(
            GQL_CREATE_UPLOAD_BUCKET_SIGNED_URLS, create_signed_urls_payload
        )
        mutation_time = time.time() - mutation_start
        if mutation_time < THROTTLING_DELAY:
            time.sleep(THROTTLING_DELAY - mutation_time)
        signed_urls = urls_response["data"]["urls"]

        used_signed_urls = 0
        urls_with_uploaded_json_content = []
        for frames_paths in self.json_content_array_batch:
            nb_frames = len(frames_paths)
            data_array = [open(path, "rb") for path in frames_paths]
            content_type_array = [mimetypes.guess_type(path)[0] for path in frames_paths]
            urls_with_uploaded_frames = upload_data_via_REST(
                signed_urls[used_signed_urls : used_signed_urls + nb_frames],
                data_array,
                content_type_array,
            )
            used_signed_urls += nb_frames
            frame_indexes = list(range(nb_frames))
            clean_urls = [cleanSignedUrl(url) for url in urls_with_uploaded_frames]
            json_content = dict(zip(frame_indexes, clean_urls))
            urls_with_uploaded_json_content.append(
                upload_data_via_REST(
                    signed_urls[used_signed_urls : used_signed_urls + 1],
                    [json_content],
                    ["application/json"],
                )
            )
            used_signed_urls += 1
        return urls_with_uploaded_json_content

    def import_local_files_to_bucket(self):
        if self.input_type in ("FRAME", "VIDEO"):
            self.json_content_array_batch = self.upload_frames_on_bucket()
        else:
            self.content_array_batch = self.upload_content_on_bucket()

    def import_raw_text_to_bucket(self):
        if self.is_uploading_json_content:
            self.json_content_array_batch = self.upload_rich_text_on_bucket()
        else:
            self.content_array_batch = self.upload_content_on_bucket()

    def get_request_payload(self) -> dict:
        is_local_files_batch = self.data_location == DataLocation.Local
        is_asynchronous_batch = self.upload_process == DataImportProcess.Asynchronous
        if is_asynchronous_batch:
            upload_type = (
                AsynchronousUploadType.GeoSatellite
                if self.input_type == "IMAGE"
                else AsynchronousUploadType.Video
            )
            payload_data = {
                "contentArray": self.content_array_batch,
                "externalIDArray": self.external_id_array_batch,
                "jsonMetadataArray": self.json_metadata_array_batch,
                "uploadType": upload_type,
                "isUploadingSignedUrl": is_local_files_batch,
            }
        else:
            payload_data = {
                "contentArray": self.content_array_batch,
                "externalIDArray": self.external_id_array_batch,
                "isHoneypotArray": self.is_honeypot_array_batch,
                "statusArray": self.status_array_batch,
                "jsonContentArray": self.json_content_array_batch,
                "jsonMetadataArray": self.json_metadata_array_batch,
                "isUploadingSignedUrl": is_local_files_batch,
            }
        return {"data": payload_data, "where": {"id": self.project_id}}

    def import_data_to_kili(self):
        request_payload = self.get_request_payload()
        mutation_start = time.time()
        result = self.auth.client.execute(self.request, request_payload)
        mutation_time = time.time() - mutation_start
        if mutation_time < THROTTLING_DELAY:
            time.sleep(THROTTLING_DELAY - mutation_time)
        return result
