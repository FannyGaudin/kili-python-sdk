from kili.adapters.kili_api_gateway import KiliAPIGateway
from kili.use_cases.label import LabelUseCases
from kili.utils.tempfile import TemporaryDirectory
from tests.shared.utils.coco import generate_coco_file_structure


def test_given_a_coco_file_when_I_call_import_label_use_case_then_it_imports_the_files(
    kili_api_gateway: KiliAPIGateway,
):
    # Given
    label_use_cases = LabelUseCases(kili_api_gateway)
    with TemporaryDirectory() as temp_dir:
        coco_file = generate_coco_file_structure(temp_dir)

        # When
        result = label_use_cases.import_labels(
            coco_file, fmt="coco", project_title="my coco import"
        )

    # Then
    assert result.project.name == "my coco import"
    assert result.project.id != ""
    assert result.assets[0].id != ""
    assert result.assets[0].external_id == "1"
