import csv
from pathlib import Path
from typing import Optional, Type

import yaml

from kili.services.label_import.exceptions import (
    MissingMetadataError,
    MissingTargetJobError,
)
from kili.services.label_import.importer._base import AbstractLabelImporter
from kili.services.label_import.parser import YoloLabelParser
from kili.services.label_import.parser._base import AbstractLabelParser
from kili.services.label_import.types import Classes, LabelFormat


class YoloLabelImporter(AbstractLabelImporter):
    """
    Label importer in the yolo format.
    """

    def _get_label_file_extension(self) -> str:
        return ".txt"

    @staticmethod
    def _check_arguments_compatibility(
        meta_file_path: Optional[Path], target_job_name: Optional[str]
    ):
        if meta_file_path is None:
            raise MissingMetadataError("Meta file is needed to import the label")
        if target_job_name is None:
            raise MissingTargetJobError("A target job name is needed to import the label")

    @staticmethod
    def _read_classes_from_meta_file(
        meta_file_path: Optional[Path], input_format: LabelFormat
    ) -> Classes:
        assert meta_file_path
        classes: Classes = Classes({})
        if input_format == "yolo_v4":
            try:
                # layout: id class\n
                with meta_file_path.open("r", encoding="utf-8") as m_f:
                    csv_reader = csv.reader(m_f, delimiter=" ")
                    classes = Classes({int(r[0]): r[1] for r in csv_reader if r[0] != " "})
            except (ValueError, IndexError):
                with meta_file_path.open("r", encoding="utf-8") as m_f:
                    classes = Classes(dict(enumerate(m_f.read().splitlines())))

        elif input_format == "yolo_v5":
            with meta_file_path.open("r", encoding="utf-8") as m_f:
                m_d = yaml.load(m_f, yaml.FullLoader)
                classes = Classes(m_d["names"])
        elif input_format == "yolo_v7":
            with meta_file_path.open("r", encoding="utf-8") as m_f:
                m_d = yaml.load(m_f, yaml.FullLoader)
                classes = Classes(dict(enumerate(m_d["names"])))
        else:
            raise NotImplementedError(f"The format f{input_format} does not have a metadata parser")

        return classes

    def _select_label_parser(self) -> Type[AbstractLabelParser]:
        return YoloLabelParser
