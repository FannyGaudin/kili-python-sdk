"""Project module."""

from tkinter.font import NORMAL
from typing import List, Optional, cast, Dict

from typing_extensions import Literal, NewType
from kili.services import convert_assets
from kili.services.conversion.typing import ExportType, LabelFormat, SplitOption

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

    def __init__(self, project_id: ProjectId, input_type: InputType, title: str, client: "kili.client.Kili"):
        self.project_id = project_id
        self.title = title
        self.input_type = input_type
        self.client = client

    def export(
        self, path_output: str, format: Format, asset_ids: Optional[List[AssetId]] = None
    ) -> None:

        if format == "yolo":

            convert_assets(
                self.client,
                asset_ids=cast(Optional[List[str]], asset_ids),
                project_id=self.project_id,
                project_title=self.title,
                export_type=ExportType.NORMAL,
                label_format=LabelFormat.YOLO_V4,
                split_option=SplitOption.SPLIT_FOLDER,
                output_file=path_output,
            )
