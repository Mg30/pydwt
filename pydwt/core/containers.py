"""
This module is defining a dependency injection container using the Dependency Injector library. 
The container has several providers which can be used to inject objects into other parts of the codebase.

config is a configuration provider that should be used to provide a dictionary of configuration settings.
database_client is a Connection provider which is used to create a database connection with the configuration provided.
datasources is a provider that returns a Datasources instance, which can be used to retrieve tables or views from the database.
cache_strategy is a provider that should be used to provide a cache strategy instance.
workflow_factory is a provider that returns a Workflow instance, which is used to execute tasks for a given DAG.
project_factory is a provider that returns a Project instance, which is a collection of tasks that can be executed together as a single unit.

"""

from dependency_injector import containers, providers
from pydwt.context.connection import Connection
from pydwt.core.workflow import Workflow
from pydwt.core.project import Project
from pydwt.context.datasources import Datasources

class Container(containers.DeclarativeContainer):
    # Configuration provider, contains the project configuration
    config = providers.Configuration()

    # Singleton provider that provides the database connection instance
    database_client = providers.ThreadSafeSingleton(
        Connection,
        config.connection.db,
        config.connection.host,
        config.connection.name,
        config.connection.password,
        config.connection.user,
        config.connection.port,
        config.connection.sql_alchemy_driver,
    )

    # Singleton provider that provides the datasources instance
    datasources = providers.ThreadSafeSingleton(
        Datasources,
        config.sources,
        database_client
    )


    # Singleton provider that provides the workflow instance
    workflow_factory = providers.ThreadSafeSingleton(
        Workflow,
    )

    # Factory provider that provides the project instance
    project_factory = providers.Factory(
        Project,
        workflow=workflow_factory,
        name=config.project.name
    )
