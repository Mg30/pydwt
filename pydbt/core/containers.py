from dependency_injector import containers, providers
from pydbt.context.connection import Connection
from pydbt.core.workflow import Workflow
from pydbt.core.cache import LocalCache
from pydbt.core.project import Project
from pydbt.context.datasources import Datasources

class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    database_client = providers.ThreadSafeSingleton(
        Connection,
        config.connection.db,
        config.connection.host,
        config.connection.name,
        config.connection.password,
        config.connection.user,
        config.connection.sql_alchemy_driver,
    )

    datasources = providers.ThreadSafeSingleton(Datasources,config.sources, database_client)

    cache_strategy = providers.Factory()
    workflow_factory = providers.ThreadSafeSingleton(
        Workflow,
        executor=config.project.executor,
        use_cache=config.project.use_cache,
        cache_strategy=LocalCache,
    )
    project_factory = providers.Factory(Project, workflow=workflow_factory,name=config.project.name)
