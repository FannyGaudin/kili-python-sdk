"""Mocks for the import_asset service test."""

from unittest.mock import MagicMock

mocked_request_signed_urls = MagicMock(
    side_effect=lambda _auth, filePaths: ["https://signed_url?id=id"] * len(filePaths)
)

mocked_upload_data_via_rest = MagicMock(side_effect=lambda signed_urls, _a, _b, _c: signed_urls)

mocked_unique_id = MagicMock(return_value="unique_id")

mocked_auth = MagicMock()


def organization_generator(upload_local_data: bool):
    yield from [{"id": "abcdefg", "license": {"uploadLocalData": upload_local_data}}]
