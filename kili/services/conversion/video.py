"""
Utility functions on video handling
"""
import logging
import os
import shutil
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Dict, List

import ffmpeg
import requests

from kili.services.conversion.repository import AbstractContentRepository
from kili.services.conversion.tools import get_endpoint_router_from_services


def cut_video(asset, frames, images_folder, orig_filename, leading_zeros):
    # pylint: disable=too-many-locals
    """
    Download and cut video into frames and save to images folder only frames that have labels
    """
    content = asset["content"].replace(
        os.getenv("ENDPOINT__ROUTER"), get_endpoint_router_from_services()
    )

    headers = None
    if asset["content"].startswith(os.getenv("ENDPOINT__ROUTER")):
        headers = {"Authorization": f"X-API-Key: {os.getenv('KILI__API_KEY')}"}

    response = requests.get(
        content, stream=True, headers=headers, verify=os.getenv("KILI__VERIFY_SSL") != "False"
    )
    if not response.ok:
        # pylint: disable=logging-too-many-args
        logging.warning("Error while downloading video of asset %s", asset["id"])
    with NamedTemporaryFile() as video_file:
        with TemporaryDirectory() as directory:
            with open(video_file.name, "wb") as fout:
                for block in response.iter_content(1024):
                    if not block:
                        break
                    fout.write(block)
            if (
                "jsonMetadata" in asset
                and "processingParameters" in asset["jsonMetadata"]
                and "framesPlayedPerSecond" in asset["jsonMetadata"]["processingParameters"]
            ):
                metadata = asset["jsonMetadata"]
                final_framerate = metadata["processingParameters"]["framesPlayedPerSecond"]
            else:
                try:
                    probe = ffmpeg.probe(video_file.name)
                    video_info = next(s for s in probe["streams"] if s["codec_type"] == "video")
                    frame_rate_string = video_info["r_frame_rate"].split("/")
                    final_framerate = int(frame_rate_string[0]) / int(frame_rate_string[1])
                except ffmpeg.Error as error:
                    stdout = error.stdout.decode("utf8")
                    stderr = error.stderr.decode("utf8")
                    logging.warning(
                        f"Error when probing video frame rate | stdout : {stdout}", asset["id"]
                    )
                    logging.warning(
                        f"Error when probing video frame rate | stderr : {stderr}", asset["id"]
                    )
                    raise error
            logging.warning(f'Used frame rate: {final_framerate} for asset with id {asset["id"]}')
            try:
                ffmpeg.input(video_file.name).filter("fps", fps=final_framerate, round="up").output(
                    os.path.join(directory, "%d.jpg"), start_number=0
                ).run(capture_stdout=True, capture_stderr=True)
            except ffmpeg.Error as error:
                stdout = error.stdout.decode("utf8")
                stderr = error.stderr.decode("utf8")
                logging.warning(
                    f"Error when extracting frames from video | stdout : {stdout}", asset["id"]
                )
                logging.warning(
                    f"Error when extracting frames from video | stderr : {stderr}", asset["id"]
                )
                raise error
            for idx, _ in frames.items():
                if os.path.isfile(os.path.join(directory, f"{idx}.jpg")):
                    shutil.copyfile(
                        os.path.join(directory, f"{idx}.jpg"),
                        os.path.join(
                            images_folder,
                            f"{orig_filename}_{str(idx + 1).zfill(leading_zeros)}.jpg",
                        ),
                    )


def get_content_frames_paths(
    asset: Dict, content_repository: AbstractContentRepository
) -> List[str]:
    """
    Get list of links to frames from the file located at asset[jsonContent]. Returns an empty list
    if `content` in the asset exists.
    """
    content_frames = []

    if not asset["content"] and asset["jsonContent"]:
        content_frames = content_repository.get_frames(asset["jsonContent"])

    return content_frames
