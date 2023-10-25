from kili.domain.label_parsing.coco import CocoParser
from kili.utils.tempfile import TemporaryDirectory
from tests.shared.utils.coco import generate_coco_file_structure


def test_given_a_coco_file_when_I_call_the_coco_parser_then_it_creates_the_data_to_import():
    # Given
    coco_parser = CocoParser()
    with TemporaryDirectory() as temp_dir:
        coco_file = generate_coco_file_structure(
            temp_dir,
            images=[{"id": 1, "file_name": "image1.jpg"}, {"id": 2, "file_name": "image2.jpg"}],
            annotations=[
                {"id": 1, "image_id": 1, "category_id": 1, "segmentation": [[1, 2, 3, 4]]},
                {"id": 2, "image_id": 1, "category_id": 2, "segmentation": [[5, 6, 7, 8]]},
                {"id": 3, "image_id": 2, "category_id": 2, "segmentation": [[1, 2, 3, 4]]},
            ],
            categories=[{"id": 1, "name": "cat1"}, {"id": 2, "name": "cat2"}],
        )

        # When
        results = coco_parser.parse(coco_file)

    # Then
    categories = [{"name": "cat1", "id": "1"}, {"name": "cat2", "id": "2"}]
    assert results.project.name == ""
    assert results.project.description == ""
    assert results.project.json_interface == {
        "jobs": {
            "SEGMENTATION_JOB": {
                "content": {"categories": categories, "input": "radio"},
                "instruction": "Segment",
                "mlTask": "OBJECT_DETECTION",
                "required": 0,
                "tools": ["semantic"],
                "isChild": False,
            }
        }
    }
    assert results.assets[0].path == str(temp_dir / "image1.jpg")
    assert results.assets[0].external_id == "1"
    assert results.assets[0].labels[0].json_response == {
        "SEGMENTATION_JOB": {
            "annotations": [
                {
                    "children": {},
                    "boundingPoly": [{"normalizedVertices": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]}],
                    "categories": [{"name": "cat1", "id": "1"}],
                    "type": "semantic",
                    "mid": "1_segm_1",
                },
                {
                    "children": {},
                    "boundingPoly": [{"normalizedVertices": [{"x": 5, "y": 6}, {"x": 7, "y": 8}]}],
                    "categories": [{"name": "cat2", "id": "2"}],
                    "type": "semantic",
                    "mid": "1_segm_2",
                },
            ]
        }
    }

    assert results.assets[1].path == str(temp_dir / "image2.jpg")
    assert results.assets[1].external_id == "2"
