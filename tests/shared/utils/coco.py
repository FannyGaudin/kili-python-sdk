from copy import deepcopy
import json
from pathlib import Path
from typing import Dict, List
from PIL import Image


def _generate_image_file(path_image: Path) -> None:
    """Generate an image file."""
    with Path.open(path_image, "w", encoding="utf-8") as f:
        Image.new("RGB", (640, 480)).save(f)


def generate_coco_file_structure(
    target_dir: Path, images: List[Dict], annotations: List[Dict], categories: List[Dict]
) -> Path:
    """Generate a coco file structure in the target directory."""

    images_coco = deepcopy(images)
    for image in images_coco:
        path_image = target_dir / image["file_name"]
        _generate_image_file(path_image)
        image["file_name"] = str(path_image)

    coco_data = {
        "info": {"year": 2021},
        "images": images_coco,
        "annotations": annotations,
        "categories": categories,
    }

    coco_filename = target_dir / "coco.json"
    with Path.open(coco_filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(coco_data))

    return coco_filename
