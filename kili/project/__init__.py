"""Project module."""
from typing import Dict, List, Optional, cast

from typing_extensions import Literal, NewType

from kili.services import convert_assets
from kili.services.conversion.typing import LabelFormat, SplitOption as SplitOptionLegacy, ExportType as ExportTypeLegacy

Format = Literal["coco", "voc", "yolo_v4", "yolo_v5"]
InputType = Literal["text", "image"]
ExportType = Literal["latest", "normal"]
AssetId = NewType("AssetId", str)
ProjectId = NewType("ProjectId", str)
SplitOption = Literal["split", "merged"]

export_type_mapping: Dict[ExportType, ExportTypeLegacy] = {"normal": ExportTypeLegacy.NORMAL, "latest": ExportTypeLegacy.LATEST}
split_mapping: Dict[SplitOption, SplitOptionLegacy] = {"merged": SplitOptionLegacy.MERGED_FOLDER, "split": SplitOptionLegacy.SPLIT_FOLDER}
format_mapping: Dict[Format, LabelFormat] = {"yolo_v4": LabelFormat.YOLO_V4, "yolo_v5": LabelFormat.YOLO_V5}

class Project:  # pylint: disable=too-few-public-methods
    """
    Object that represents a project in Kili.
    It allows management operations such as uploading assets, uploading predictions,
    modifying the project's queue etc.
    It also allows queries from this project such as its assets, labels etc.
    """

    def __init__(
        self, project_id: ProjectId, input_type: InputType, title: str, client: "kili.client.Kili"
    ):
        self.project_id = project_id
        self.title = title
        self.input_type = input_type
        self.client = client

    def export(
        self, path_output: str, output_format: Format, asset_ids: Optional[List[AssetId]] = None, export_type: ExportType = "normal", split_option: SplitOption = "split"
    ) -> None:
        if output_format in ["yolo_v4", "yolo_v5"]:
            convert_assets(
                self.client,
                asset_ids=cast(Optional[List[str]], asset_ids),
                project_id=self.project_id,
                project_title=self.title,
                export_type=export_type_mapping[export_type],
                label_format=format_mapping[output_format],
                split_option=split_mapping[split_option],
                output_file=path_output,
            )
