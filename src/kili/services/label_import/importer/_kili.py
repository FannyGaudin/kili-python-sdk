import json
from pathlib import Path
from typing import List, Optional, Type

from kili.services.label_import.helpers import import_labels_from_dict
from kili.services.label_import.importer._base import AbstractLabelImporter
from kili.services.label_import.parser import KiliRawLabelParser
from kili.services.label_import.parser._base import AbstractLabelParser
from kili.services.label_import.types import Classes, LabelFormat
from kili.services.types import ProjectId


class KiliRawLabelImporter(AbstractLabelImporter):
    """
    # Label importer in the Kili format.
    """

    def _get_label_file_extension(self) -> str:
        return ".json"

    @staticmethod
    def _check_arguments_compatibility(
        meta_file_path: Optional[Path], target_job_name: Optional[str]
    ):
        pass

    @classmethod
    def _read_classes_from_meta_file(
        cls, meta_file_path: Optional[Path], input_format: LabelFormat
    ) -> Optional[Classes]:
        return None

    def _select_label_parser(self) -> Type[AbstractLabelParser]:
        return KiliRawLabelParser


class KiliRawSingleFileLabelImporter(AbstractLabelImporter):
    def process_from_files(
        self,
        labels_file_path: Optional[Path],
        labels_files: Optional[List[Path]],
        meta_file_path: Optional[Path],
        project_id: ProjectId,
        target_job_name: Optional[str],
        model_name: Optional[str],
        is_prediction: bool,
    ):
        _ = labels_file_path, meta_file_path, project_id, target_job_name, model_name
        assert labels_files

        import_labels_from_dict(
            self.kili,
            json.load(labels_files[0].open(encoding="utf-8")),
            label_type="DEFAULT" if not is_prediction else "PREDICTION",
        )
