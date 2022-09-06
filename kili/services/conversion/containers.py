

from dependency_injector import containers, providers
from kili.services.conversion.format.base import (
    ExportParams,
)
from kili.services.conversion.logger import (
    AbstractLogger
)
from kili.services.conversion.repository import (
    AbstractContentRepository
)


class Service:

    def __init__(self, kili, logger: AbstractLogger, content_repository: AbstractContentRepository) -> None:
        self.kili = kili  # <-- dependency is injected
        self.logger = logger
        self.content_repository = content_repository

class Container(containers.DeclarativeContainer):

    config = providers.Configuration()

    service = providers.Factory(
        Service,
        api_client=api_client,
    )
