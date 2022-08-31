"""
Fake Kili object
"""

from test.services.export.fakes.fake_data import asset
from typing import List, Optional


class FakeKili(object):
    """
    Handke .assets and .project methods of Kili
    """

    def assets(
        self,
        project_id: str,
        fields: List[str],
        label_type_in: Optional[List[str]] = None,
        asset_id_in: Optional[List[str]] = None,
    ):
        """
        Fake assets
        """
        _ = fields, label_type_in, asset_id_in
        if project_id == "1bb":
            return [asset]
        else:
            return []

    def projects(self, project_id: str, fields: List[str], disable_tqdm: bool = False):
        """
        Fake projects
        """
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
