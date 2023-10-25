"""
Coco format parser.

{
    "info": {
        "description": "COCO 2017 Dataset","url": "http://cocodataset.org","version": "1.0","year": 2017,"contributor": "COCO Consortium","date_created": "2017/09/01"
    },
    "licenses": [
        {"url": "http://creativecommons.org/licenses/by/2.0/","id": 4,"name": "Attribution License"}
    ],
    "images": [
        {"id": 242287, "license": 4, "coco_url": "http://images.cocodataset.org/val2017/xxxxxxxxxxxx.jpg", "flickr_url": "http://farm3.staticflickr.com/2626/xxxxxxxxxxxx.jpg", "width": 426, "height": 640, "file_name": "xxxxxxxxx.jpg", "date_captured": "2013-11-15 02:41:42"},
        {"id": 245915, "license": 4, "coco_url": "http://images.cocodataset.org/val2017/nnnnnnnnnnnn.jpg", "flickr_url": "http://farm1.staticflickr.com/88/xxxxxxxxxxxx.jpg", "width": 640, "height": 480, "file_name": "nnnnnnnnnn.jpg", "date_captured": "2013-11-18 02:53:27"}
    ],
    "annotations": [
        {"id": 125686, "category_id": 0, "iscrowd": 0, "segmentation": [[164.81, 417.51,......167.55, 410.64]], "image_id": 242287, "area": 42061.80340000001, "bbox": [19.23, 383.18, 314.5, 244.46]},
        {"id": 1409619, "category_id": 0, "iscrowd": 0, "segmentation": [[376.81, 238.8,........382.74, 241.17]], "image_id": 245915, "area": 3556.2197000000015, "bbox": [399, 251, 155, 101]},
        {"id": 1410165, "category_id": 1, "iscrowd": 0, "segmentation": [[486.34, 239.01,..........495.95, 244.39]], "image_id": 245915, "area": 1775.8932499999994, "bbox": [86, 65, 220, 334]}
    ],
    "categories": [
        {"supercategory": "speaker","id": 0,"name": "echo"},
        {"supercategory": "speaker","id": 1,"name": "echo dot"}
    ]
}
"""

import json
from pathlib import Path
from typing import Dict
from kili.domain.asset.asset import AssetExternalId
from kili.domain.label_importer import (
    KiliAssetToImport,
    KiliDataToImport,
    KiliLabelToImport,
    KiliProjectToImport,
)
from kili.domain.label_parsing.base import AbstractLabelingDataParser


class CocoParser(AbstractLabelingDataParser):
    def parse(self, label_file: Path) -> KiliDataToImport:
        """Parses a label file and returns the data to import into Kili"""
        coco_dict = self._load_coco_dict(label_file)
        project = self._parse_project(coco_dict)
        labels_by_image_id = self._parse_labels(coco_dict)
        category_id_to_name = self._parse_categories(coco_dict)
        assets = self._parse_assets(coco_dict, labels_by_image_id, category_id_to_name)

        return KiliDataToImport(
            project,
            assets,
        )

    def _load_coco_dict(self, label_file: Path) -> Dict:
        with label_file.open(encoding="utf-8") as f:
            return json.load(f)

    def _parse_project(self, coco_dict: Dict):
        title = coco_dict.get("info", {}).get("description", "")
        json_interface = {
            "jobs": {
                "SEGMENTATION_JOB": {
                    "content": {
                        "categories": [
                            {"name": category["name"], "id": str(category["id"])}
                            for category in coco_dict.get("categories", [])
                        ],
                        "input": "radio",
                    },
                    "instruction": "Segment",
                    "mlTask": "OBJECT_DETECTION",
                    "required": 0,
                    "tools": ["semantic"],
                    "isChild": False,
                }
            }
        }
        return KiliProjectToImport(title, "", "IMAGE", json_interface)

    def _parse_labels(self, coco_dict: Dict):
        labels_by_image_id = {}
        for annotation in coco_dict.get("annotations", []):
            image_id = annotation["image_id"]
            labels_by_image_id[image_id] = labels_by_image_id.get(image_id, [])
            labels_by_image_id[image_id].append(annotation)
        return labels_by_image_id

    def _parse_assets(self, coco_dict: Dict, labels_by_image_id: Dict, category_id_to_name: Dict):
        assets = []
        for image in coco_dict.get("images", []):
            image_id = image["id"]
            coco_image_annotations = labels_by_image_id.get(image_id, [])
            kili_image_annotations = [
                {
                    "children": {},
                    "boundingPoly": [{"normalizedVertices": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]}],
                    "categories": [
                        {
                            "name": category_id_to_name[coco_image_annotation["category_id"]],
                            "id": str(coco_image_annotation["category_id"]),
                        }
                    ],
                    "type": "semantic",
                    "mid": f"{image_id}_segm_{ind}",
                }
                for ind, coco_image_annotation in enumerate(coco_image_annotations)
            ]

            asset = KiliAssetToImport(
                AssetExternalId(str(image_id)),
                image["file_name"],
                [KiliLabelToImport({"SEGMENTATION_JOB": {"annotations": kili_image_annotations}})],
            )
            assets.append(asset)
        return assets

    def _parse_categories(self, coco_dict: Dict) -> Dict:
        return {category["id"]: category["name"] for category in coco_dict.get("categories", [])}
