"""Service module for projects."""

from typing import Dict, List

from kili.core.graphql import QueryOptions
from kili.core.graphql.graphql_client import GraphQLClient
from kili.core.graphql.operations.project.queries import ProjectQuery, ProjectWhere
from kili.exceptions import NotFound


def get_project(project_id: str, fields: List[str], graphql_client: GraphQLClient) -> Dict:
    """Get a project from its id or raise a NotFound Error if not found."""
    projects = list(
        ProjectQuery(graphql_client)(
            ProjectWhere(project_id=project_id), fields, QueryOptions(disable_tqdm=True, first=1)
        )
    )
    if len(projects) == 0:
        raise NotFound(
            f"project ID: {project_id}. Maybe your KILI_API_KEY does not belong to a member of the"
            " project."
        )
    return projects[0]


def get_project_field(project_id: str, field: str, graphql_client: GraphQLClient):
    """Get one project field from a the project id.

    Raise a NotFound Error if the project is not found.
    """
    return get_project(project_id, [field], graphql_client)[field]
