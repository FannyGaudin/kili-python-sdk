"""
Gets the
"""
from abc import ABC, abstractmethod
import requests
from typing import Iterator, List, Any, Dict, Optional


class DownloadError(Exception):
    """
    Exception thrown when the contents cannot be downloaded.
    """



class AbstractContentRepository(ABC):
    """
    Interface to the content repository
    """

    def __init__(self, router_endpoint: str, router_headers: Dict[str, str], verify_ssl: bool) -> None:
        self.router_endpoint = router_endpoint
        self.router_headers = router_headers
        self.verify_ssl = verify_ssl

    @abstractmethod
    def get_frames(self, content_url: str) -> List[str]:
        pass

    @abstractmethod
    def get_content_stream(self, content_url: str, block_size: int) -> Iterator[Any]:
        pass


class SDKContentRepository(AbstractContentRepository):
    """
    Handle content fetching from the server from the SDK
    """

    def get_frames(self, content_url: str) ->  List[str]:
        frames: List[str] = []
        headers = None
        if content_url.startswith(self.router_endpoint):
            headers = self.router_headers
        json_content_resp = requests.get(
            content_url, headers=headers, verify=self.verify_ssl
        )

        if json_content_resp.ok:
            frames = list(json_content_resp.json().values())
        return frames

    def get_content_stream(self, content_url: str, block_size: int) -> Iterator[Any]:

        response = requests.get(
            content_url,
            stream=True,
            headers=self.router_headers,
            verify=self.verify_ssl
        )
        if not response.ok:
            raise DownloadError(f"Error while downloading image {content_url}") # pylint: disable=logging-too-many-args

        return response.iter_content(block_size)
