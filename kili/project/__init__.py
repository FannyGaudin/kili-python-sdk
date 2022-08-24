"""Project module."""

from typing import List, Optional, cast

from typing_extensions import Literal, NewType

Format = Literal["coco", "voc", "yolo"]
InputType = Literal["TEXT", "IMAGE"]
AssetId = NewType("AssetId", str)
ProjectId = NewType("ProjectId", str)


class Project:  # pylint: disable=too-few-public-methods
    """
    Object that represents a project in Kili.
    It allows management operations such as uploading assets, uploading predictions,
    modifying the project's queue etc.
    It also allows queries from this project such as its assets, labels etc.
    """

    def __init__(self, project_id: ProjectId, input_type: InputType, title: str):
        self.project_id = project_id
        self.title = title
        self.input_type = input_type

    def export(
        self, client, path_output: str, format: Format, asset_ids: Optional[List[AssetId]]
    ) -> None:
        _ = client.assets(
            project_id=self.project_id, asset_id_in=cast(Optional[List[str]], asset_ids)
        )

        if format == "yolo":
            pass
