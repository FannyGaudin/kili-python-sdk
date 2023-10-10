"""Hi Kili Team,.

I have a question about expected behavior when using the asset() function and specifying multiple label_* filters. Will the asset be returned if it has labels that each meet part of the criteria or only if it has a label that meets all the criteria?

For example, if we're using the following filters:

asset(

  label_created_at_gte="2023-09-27",

  label_created_at_lte="2023-09-28",

  label_type_in=["REVIEW"]

)

Which of the following assets would be returned?

asset1:

  label {type: "REVIEW", created_at: "2023-09-20"}

  label {type: "DEFAULT", created_at: "2023-09-29"}

(first label matches label_type_in and label_created_at_lte, second label matches label_created_at_gte)

asset2:

  label {type: "REVIEW", created_at: "2023-09-20"}

  label {type: "DEFAULT", created_at: "2023-09-27"}

(first label matches label_type_in, second label matches the combined created_at criteria)

asset3:

  label {type: "REVIEW", created_at: "2023-09-27"}

  label {type: "DEFAULT", created_at: "2023-09-29"}

(first label meets all criteria)

Finally, would this same behavior apply when using the GraphQL api as well?

Thanks,

Nicole

Software Engineer, EvenUp
"""

import time
from datetime import datetime
from typing import Any, Dict, List, cast

import pytest
import pytz

from kili.client import Kili


@pytest.fixture()
def kili():
    return Kili()


@pytest.fixture()
def project_id(kili: Kili):
    project_id = kili.create_project(
        title="test_project",
        json_interface={
            "jobs": {
                "CLASSIFICATION_JOB": {
                    "content": {
                        "categories": {
                            "A": {"children": [], "name": "A", "id": "category5"},
                            "B": {"children": [], "name": "B", "id": "category6"},
                        },
                        "input": "radio",
                    },
                    "instruction": "class",
                    "mlTask": "CLASSIFICATION",
                    "required": 0,
                    "isChild": False,
                    "isNew": False,
                }
            }
        },
        input_type="TEXT",
    )["id"]

    yield project_id

    kili.delete_project(project_id)


@pytest.fixture()
def asset_data():
    return [
        {
            "content": "hello",
            "labels": [
                {"type": "REVIEW", "toBeCreated": "before"},
                {"type": "DEFAULT", "toBeCreated": "after"},
            ],
        },
        {
            "content": "hello again",
            "labels": [
                {"type": "REVIEW", "toBeCreated": "before"},
                {"type": "DEFAULT", "toBeCreated": "now"},
            ],
        },
        {
            "content": "hello ?",
            "labels": [
                {"type": "REVIEW", "toBeCreated": "now"},
                {"type": "DEFAULT", "toBeCreated": "after"},
            ],
        },
    ]


@pytest.fixture()
def asset_ids(project_id: Any, kili: Kili):
    return cast(
        dict,
        kili.append_many_to_dataset(
            project_id=project_id,
            content_array=["hello", "hello again", "hello ?"],
        ),
    )["asset_ids"]


def _create_labels(
    asset_data: List[Dict], asset_ids: List[str], when: str, project_id: str, kili: Kili
):
    zip(
        [
            kili.append_labels(
                project_id=project_id,
                asset_id_array=[asset_id],
                json_response_array=[
                    {"CLASSIFICATION_JOB": {"categories": [{"confidence": 100, "name": "A"}]}}
                ],
                label_type=label["type"],
                disable_tqdm=True,
            )[0]["id"]
            for asset, asset_id in zip(asset_data, asset_ids)
            for label in asset["labels"]
            if label["toBeCreated"] == when
        ],
        asset_ids,
    )

    kili.assets(
        project_id=project_id,
        asset_id_in=asset_ids,
        fields=["labels.jsonResponse", "labels.labelType"],
    )


def test_given_assets_with_labels_when_I_filter_over_several_label_properties_then_I_get_the_correct_assets_and_labels(
    asset_ids: List[str], asset_data: List[Dict[str, Any]], kili: Kili, project_id: str
):
    _create_labels(asset_data, asset_ids, "before", project_id, kili)
    time.sleep(5)
    now_time = datetime.now(pytz.utc)
    _create_labels(asset_data, asset_ids, "now", project_id, kili)
    time.sleep(5)
    _create_labels(asset_data, asset_ids, "after", project_id, kili)

    print("assets before, default")
    assets = kili.assets(
        project_id=project_id,
        label_created_at_lte=now_time.strftime("%Y-%m-%dT%H:%M:%S"),
        label_type_in=["DEFAULT"],
        fields=["labels.jsonResponse", "labels.labelType"],
    )
    print(assets)

    print("assets before, review")
    assets = kili.assets(
        project_id=project_id,
        label_created_at_lte=now_time.strftime("%Y-%m-%dT%H:%M:%S"),
        label_type_in=["REVIEW"],
        fields=["labels.jsonResponse", "labels.labelType"],
    )
    print(assets)

    print("assets after, default")
    assets = kili.assets(
        project_id=project_id,
        label_created_at_gte=now_time.strftime("%Y-%m-%dT%H:%M:%S"),
        fields=["labels.jsonResponse", "labels.labelType"],
        label_type_in=["DEFAULT"],
    )
    print(assets)

    print("assets after, review")
    assets = kili.assets(
        project_id=project_id,
        label_created_at_gte=now_time.strftime("%Y-%m-%dT%H:%M:%S"),
        fields=["labels.jsonResponse", "labels.labelType"],
        label_type_in=["REVIEW"],
    )
    print(assets)

    kili.delete_project(project_id=project_id)
