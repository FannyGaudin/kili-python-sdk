import json
from pathlib import Path
from PIL import Image


def _generate_image_file(path_image: Path) -> None:
    """Generate an image file."""
    with Path.open(path_image, "w", encoding="utf-8") as f:
        Image.new("RGB", (640, 480)).save(f)


def generate_coco_file_structure(target_dir: Path) -> Path:
    """Generate a coco file structure in the target directory."""

    path_image_1 = target_dir / "image1.jpg"
    _generate_image_file(path_image_1)

    path_image_2 = target_dir / "image2.jpg"
    _generate_image_file(path_image_2)

    coco_data = {
        "info": {"year": 2021},
        "images": [{"id": 1, "file_name": path_image_1}, {"id": 2, "file_name": path_image_2}],
        "annotations": [
            {"id": 1, "image_id": 1, "category_id": 1},
            {"id": 2, "image_id": 2, "category_id": 2},
        ],
        "categories": [{"id": 1, "name": "cat1"}, {"id": 2, "name": "cat2"}],
    }

    coco_filename = target_dir / "coco.json"
    with Path.open(coco_filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(coco_data))

    return coco_filename
