from dataclasses import dataclass
from pydwt.sql.session import Session
from pydwt.context.connection import Connection
from typing import Any, Dict


@dataclass
class Datasources(object):
    """Class for accessing tables in a SQL database.

    Attributes:
        referentiel (dict): A dictionary that contains metadata about the database.
        engine (Any): A SQLAlchemy engine object that connects to the database.
    """

    referentiel: Dict[str, dict]
    connection: Connection

    def get_source(self, name: str):
        """Returns a SQLAlchemy Table object for a given data source.

        Args:
            name (str): The name of the data source to retrieve.

        Returns:
            DataFrame: A DataFrame object representing the data source.
        """
        # Get the configuration for the data source
        config = self.referentiel[name]
        engine = self.connection.get_engine()
        # Create a Session object for the schema that contains the table
        session = Session(engine=engine, schema=config["schema"])

        # Return a Table object for the table in the data source
        return session.table(config["table"])