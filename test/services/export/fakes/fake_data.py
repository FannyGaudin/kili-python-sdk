from typing import Dict

from kili.services.conversion.typing import JobCategory

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
