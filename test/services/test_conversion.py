import glob
import os
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional
from unittest import TestCase
from zipfile import ZipFile

from kili.orm import AnnotationFormat
from kili.services import convert_assets
from kili.services.conversion.format.yolo import (
    _convert_from_kili_to_yolo_format,
    _process_asset_for_job,
    _write_class_file,
)
from kili.services.conversion.typing import (
    ExportType,
    JobCategory,
    LabelFormat,
    SplitOption,
)

job_category_a: JobCategory = JobCategory(category_name="OBJECT_A", id=0, job_id="JOB_0")
job_category_b: JobCategory = JobCategory(category_name="OBJECT_B", id=1, job_id="JOB_0")
category_ids: Dict[str, JobCategory] = {
    "JOB_0__OBJECT_A": job_category_a,
    "JOB_0__OBJECT_B": job_category_b,
}
job_0 = {
    "JOB_0": {
        "annotations": [
            {
                "categories": [{"confidence": 100, "name": "OBJECT_A"}],
                "jobName": "JOB_0",
                "mid": "2022040515434712-7532",
                "mlTask": "OBJECT_DETECTION",
                "boundingPoly": [
                    {
                        "normalizedVertices": [
                            {"x": 0.16504140348233334, "y": 0.7986938935103378},
                            {"x": 0.16504140348233334, "y": 0.2605618833516984},
                            {"x": 0.8377886490672706, "y": 0.2605618833516984},
                            {"x": 0.8377886490672706, "y": 0.7986938935103378},
                        ]
                    }
                ],
                "type": "rectangle",
                "children": {},
            }
        ]
    }
}
asset = {
    "latestLabel": {
        "jsonResponse": job_0,
        "author": {"firstname": "Jean-Pierre", "lastname": "Dupont"},
    },
    "externalId": "car_1",
    "content": "https://storage.googleapis.com/label-public-staging/car/car_1.jpg",
    "jsonContent": "",
}

asset_frame = {
    "latestLabel": {
        "jsonResponse": {
            "0": job_0,
            "1": job_0,
            "2": job_0,
            "3": job_0,
        }
    },
    "externalId": "video_1",
    "content": "https://storage.googleapis.com/label-public-staging/video1/video1.mp4",
    "jsonContent": "",
}


class FakeKili(object):
    def assets(
        self,
        project_id: str,
        fields: List[str],
        label_type_in: Optional[List[str]] = None,
        asset_id_in: Optional[List[str]] = None,
    ):
        _ = fields, label_type_in, asset_id_in
        if project_id == "1bb":
            return [asset]
        else:
            return []

    def projects(self, project_id: str, fields: List[str], disable_tqdm: bool = False):
        _ = fields, disable_tqdm
        if project_id == "1bb":
            job_payload = {
                "mlTask": "OBJECT_DETECTION",
                "tools": ["rectangle"],
                "instruction": "Categories",
                "required": 1,
                "isChild": False,
                "content": {
                    "categories": {
                        "OBJECT_A": {
                            "name": "OBJECT A",
                        },
                        "OBJECT_B": {
                            "name": "OBJECT B",
                        },
                    },
                    "input": "radio",
                },
            }
            json_interface = {
                "jobs": {
                    "JOB_0": job_payload,
                    "JOB_1": job_payload,
                    "JOB_2": job_payload,
                    "JOB_3": job_payload,
                }
            }
            return [
                {
                    "title": "test project",
                    "id": "1bb",
                    "description": "This is a test project",
                    "jsonInterface": json_interface,
                }
            ]
        else:
            return []


def get_file_tree(folder: str):
    dct = {}
    filepaths = [
        f.replace(os.path.join(folder, ""), "")
        for f in glob.iglob(folder + "**/**", recursive=True)
    ]
    for f in filepaths:
        p = dct
        for x in f.split("/"):
            if len(x):
                p = p.setdefault(x, {})
    return dct


class YoloTestCase(TestCase):
    def test_process_asset_for_job_image_not_served_by_kili(self):
        with TemporaryDirectory() as images_folder:
            with TemporaryDirectory() as labels_folder:
                asset_remote_content, video_filenames = _process_asset_for_job(
                    asset, images_folder, labels_folder, category_ids
                )

                nb_files = len(
                    [
                        name
                        for name in os.listdir(labels_folder)
                        if os.path.isfile(os.path.join(labels_folder, name))
                    ]
                )

                self.assertTrue(os.path.isfile(os.path.join(labels_folder, "car_1.txt")))
                self.assertEqual(nb_files, 1)
                self.assertEqual(
                    asset_remote_content,
                    [
                        [
                            "car_1",
                            "https://storage.googleapis.com/label-public-staging/car/car_1.jpg",
                            "car_1.txt",
                        ]
                    ],
                )
                self.assertEqual(len(video_filenames), 0)

    def test_process_asset_for_job_frame_not_served_by_kili(self):
        with TemporaryDirectory() as images_folder:
            with TemporaryDirectory() as labels_folder:
                asset_remote_content, video_filenames = _process_asset_for_job(
                    asset_frame, images_folder, labels_folder, category_ids
                )

                nb_files = len(
                    [
                        name
                        for name in os.listdir(labels_folder)
                        if os.path.isfile(os.path.join(labels_folder, name))
                    ]
                )

                self.assertEqual(nb_files, 4)

                for i in range(nb_files):
                    self.assertTrue(
                        os.path.isfile(os.path.join(labels_folder, f"video_1_{i+1}.txt"))
                    )

                expected_content = [
                    [
                        "video_1",
                        "https://storage.googleapis.com/label-public-staging/video1/video1.mp4",
                        f"video_1_{i+1}.txt",
                    ]
                    for i in range(4)
                ]
                self.assertEqual(asset_remote_content, expected_content)

                expected_video_filenames = [f"video_1_{i+1}" for i in range(4)]
                self.assertEqual(len(video_filenames), 4)
                self.assertEqual(video_filenames, expected_video_filenames)

    def test_convert_from_kili_to_yolo_format(self):
        converted_annotations = _convert_from_kili_to_yolo_format(
            "JOB_0", asset["latestLabel"], category_ids
        )
        expected_annotations = [
            (0, 0.501415026274802, 0.5296278884310182, 0.6727472455849373, 0.5381320101586394)
        ]
        self.assertEqual(len(converted_annotations), 1)
        self.assertEqual(converted_annotations, expected_annotations)

    def test_write_class_file_yolo_v4(self):
        with TemporaryDirectory() as directory:
            _write_class_file(directory, category_ids, AnnotationFormat.YoloV4)
            self.assertTrue(os.path.isfile(os.path.join(directory, "classes.txt")))
            with open(os.path.join(directory, "classes.txt"), "rb") as created_file:
                with open("./test/services/expected/classes.txt", "rb") as expected_file:
                    self.assertEqual(expected_file.read(), created_file.read())

    def test_write_class_file_yolo_v5(self):
        with TemporaryDirectory() as directory:
            _write_class_file(directory, category_ids, AnnotationFormat.YoloV5)
            self.assertTrue(os.path.isfile(os.path.join(directory, "data.yaml")))
            with open(os.path.join(directory, "data.yaml"), "rb") as created_file:
                with open("./test/services/expected/data.yaml", "rb") as expected_file:
                    self.assertEqual(expected_file.read(), created_file.read())

    def test_conversion_service_split(self):
        with TemporaryDirectory() as export_folder:
            with TemporaryDirectory() as extract_folder:

                path_zipfile = Path(export_folder) / "export.zip"
                path_zipfile.parent.mkdir(parents=True, exist_ok=True)

                fake_kili = FakeKili()
                convert_assets(
                    fake_kili,
                    asset_ids=[],
                    project_id="1bb",
                    project_title="test project",
                    export_type=ExportType.LATEST,
                    label_format=LabelFormat.YOLO_V5,
                    split_option=SplitOption.SPLIT_FOLDER,
                    output_file=str(path_zipfile),
                )

                Path(extract_folder).mkdir(parents=True, exist_ok=True)
                with ZipFile(path_zipfile, "r") as z:
                    z.extractall(extract_folder)

                file_tree_result = get_file_tree(extract_folder)

                file_tree_expected = {
                    "images": {"remote_assets.csv": {}},
                    "JOB_0": {
                        "labels": {
                            "car_1.txt": {},
                        },
                        "data.yaml": {},
                    },
                    "JOB_1": {"labels": {}, "data.yaml": {}},
                    "JOB_2": {"labels": {}, "data.yaml": {}},
                    "JOB_3": {"labels": {}, "data.yaml": {}},
                    "README.kili.txt": {},
                }

                assert file_tree_result == file_tree_expected

    def test_conversion_service_nonsplit(self):

        with TemporaryDirectory() as export_folder:
            with TemporaryDirectory() as extract_folder:

                path_zipfile = Path(export_folder) / "export.zip"
                path_zipfile.parent.mkdir(parents=True, exist_ok=True)

                fake_kili = FakeKili()
                convert_assets(
                    fake_kili,
                    asset_ids=[],
                    project_id="1bb",
                    project_title="test project",
                    export_type=ExportType.LATEST,
                    label_format=LabelFormat.YOLO_V5,
                    split_option=SplitOption.MERGED_FOLDER,
                    output_file=str(path_zipfile),
                )

                Path(extract_folder).mkdir(parents=True, exist_ok=True)
                with ZipFile(path_zipfile, "r") as z:
                    z.extractall(extract_folder)

                file_tree_result = get_file_tree(extract_folder)

                file_tree_expected = {
                    "images": {"remote_assets.csv": {}},
                    "labels": {"car_1.txt": {}},
                    "data.yaml": {},
                    "README.kili.txt": {},
                }

                assert file_tree_result == file_tree_expected
