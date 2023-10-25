"""Label importer domain."""
from dataclasses import dataclass
from typing import List
from kili.domain.asset.asset import AssetExternalId, AssetId

from kili.domain.project import InputType, ProjectId


@dataclass(frozen=True)
class ImportedProject:
    """Project imported from a label file."""

    id: ProjectId  # noqa: A003
    name: str


@dataclass(frozen=True)
class ImportedAsset:
    """Asset imported from a label file."""

    id: AssetId  # noqa: A003
    external_id: AssetExternalId


@dataclass(frozen=True)
class LabelImporterResult:
    """Result of a label import."""

    project: ImportedProject
    assets: List[ImportedAsset]


@dataclass(frozen=True)
class KiliProjectToImport:
    """Input project for the label importer."""

    name: str
    description: str
    input_type: InputType
    json_interface: dict


@dataclass(frozen=True)
class KiliLabelToImport:
    """Input labels for the label importer."""

    json_response: dict


@dataclass(frozen=True)
class KiliAssetToImport:
    """Input assets for the label importer."""

    external_id: AssetExternalId
    path: str
    labels: List[KiliLabelToImport]


@dataclass(frozen=True)
class KiliDataToImport:
    """Input assets for the label importer."""

    project: KiliProjectToImport
    assets: List[KiliAssetToImport]
