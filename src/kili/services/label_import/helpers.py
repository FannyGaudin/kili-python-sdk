"""
Helpers to perform the import
"""
from json import dumps
from typing import Dict, List

from kili.graphql.operations.label.mutations import GQL_APPEND_MANY_LABELS
from kili.helpers import format_result
from kili.orm import Label
from kili.services.label_import.types import _LabelsValidator
from kili.services.types import LabelType
from kili.utils import pagination


def import_labels_from_dict(kili, labels: List[Dict], label_type: LabelType):
    """
    Import the labels from dictionaries
    """
    _LabelsValidator(labels=labels)
    labels_data = [
        {
            "jsonResponse": dumps(label.get("json_response")),
            "assetID": label.get("asset_id"),
            "secondsToLabel": label.get("seconds_to_label"),
            "modelName": label.get("model_name"),
            "authorID": label.get("author_id"),
        }
        for label in labels
    ]
    batch_generator = pagination.batch_iterator_builder(labels_data)
    result = []
    for batch_labels in batch_generator:
        variables = {
            "data": {"labelType": label_type, "labelsData": batch_labels},
            "where": {"idIn": [label["assetID"] for label in batch_labels]},
        }
        batch_result = kili.auth.client.execute(GQL_APPEND_MANY_LABELS, variables)
        result.extend(format_result("data", batch_result, Label))
    return result
