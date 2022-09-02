"""
Handles the Yolo export with the split layout
"""

import os
import shutil
from tempfile import TemporaryDirectory
from typing import Dict, List

from kili.orm import AnnotationFormat
from kili.services.conversion.format.yolo.common import (
    YoloFormatExporter,
    get_category_full_name,
    get_project_and_init,
    write_labels_into_single_folder,
)
from kili.services.conversion.tools import create_readme_kili_file
from kili.services.conversion.typing import JobCategory


class YoloSplitExporter(YoloFormatExporter):
    def __init__(self, project_id, export_type, label_format, disable_tqdm, kili, logger):
        super().__init__(project_id, export_type, label_format, disable_tqdm, kili, logger)

    def process_and_save(self, assets: List[Dict], output_filename: str) -> None:

        self.logger.warning("Exporting to yolo format split...")

        json_interface, ml_task, tool = get_project_and_init(self.kili, self.project_id)
        categories_by_job = self._get_categories_by_job(json_interface, ml_task, tool)

        with TemporaryDirectory() as root_folder:
            images_folder = os.path.join(root_folder, self.project_id, "images")
            os.makedirs(images_folder)
            self._write_jobs_labels_into_split_folders(
                assets,
                categories_by_job,
                root_folder,
                images_folder,
                self.project_id,
                self.label_format,
                self.disable_tqdm,
            )
            create_readme_kili_file(
                self.kili, root_folder, self.project_id, self.label_format, self.export_type
            )
            path_folder = os.path.join(root_folder, self.project_id)
            path_archive = shutil.make_archive(path_folder, "zip", path_folder)
            shutil.copy(path_archive, output_filename)

        self.logger.warning("Done!")

    # def process_and_save_yolo_pytorch_export_split(
    #     self,
    #     kili,
    #     export_type: ExportType,
    #     assets: List[Dict],
    #     project_id: str,
    #     label_format: AnnotationFormat,
    #     logger,
    #     output_filename: str,
    #     disable_tqdm: bool,
    # ) -> None:
    #     # pylint: disable=too-many-locals, too-many-arguments
    #     """
    #     Save the assets and annotations to a zip file in the Yolo format.
    #     Split each job in a different folder with its own class file.
    #     """
    #     logger = logging.getLogger("kili.services.conversion.export")
    #     logger.warning("Exporting to yolo format split...")

    #     json_interface, ml_task, tool = get_project_and_init(kili, project_id)
    #     categories_by_job = self._get_categories_by_job(json_interface, ml_task, tool)

    #     with TemporaryDirectory() as root_folder:
    #         images_folder = os.path.join(root_folder, project_id, "images")
    #         os.makedirs(images_folder)
    #         self._write_jobs_labels_into_split_folders(
    #             assets,
    #             categories_by_job,
    #             root_folder,
    #             images_folder,
    #             project_id,
    #             label_format,
    #             disable_tqdm,
    #         )
    #         create_readme_kili_file(kili, root_folder, project_id, label_format, export_type)
    #         path_folder = os.path.join(root_folder, project_id)
    #         path_archive = shutil.make_archive(path_folder, "zip", path_folder)
    #         shutil.copy(path_archive, output_filename)

    #     logger.warning("Done!")

    def _get_categories_by_job(
        self, json_interface: Dict, ml_task: str, tool: str
    ) -> Dict[str, Dict[str, JobCategory]]:
        """
        Return a dictionary of JobCategory instances by category full name and job id.
        """
        categories_by_job: Dict[str, Dict[str, JobCategory]] = {}
        for job_id, job in json_interface.get("jobs", {}).items():
            if (
                job.get("mlTask") != ml_task
                or tool not in job.get("tools", [])
                or job.get("isModel")
            ):
                continue
            categories: Dict[str, JobCategory] = {}
            for cat_id, category in enumerate(job.get("content", {}).get("categories", {})):
                categories[get_category_full_name(job_id, category)] = JobCategory(
                    category_name=category, id=cat_id, job_id=job_id
                )
            categories_by_job[job_id] = categories
        return categories_by_job

    def _write_jobs_labels_into_split_folders(
        self,
        assets: List[Dict],
        categories_by_job: Dict[str, Dict[str, JobCategory]],
        root_folder: str,
        images_folder: str,
        project_id: str,
        label_format: AnnotationFormat,
        disable_tqdm: bool,
    ) -> None:
        """
        Write assets into split folders.
        """
        for job_id, category_ids in categories_by_job.items():

            base_folder = os.path.join(root_folder, project_id, job_id)
            labels_folder = os.path.join(base_folder, "labels")
            os.makedirs(labels_folder)

            write_labels_into_single_folder(
                assets,
                category_ids,
                labels_folder,
                images_folder,
                base_folder,
                label_format,
                disable_tqdm,
            )
