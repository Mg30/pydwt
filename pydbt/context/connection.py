from dataclasses import dataclass, field
from sqlalchemy import create_engine, Engine
from urllib.parse import quote_plus


@dataclass
class Connection(object):
    """Class representing a connection to a database.

    Attributes:
        db (str): Name of the database.
        host (int): Hostname or IP address of the server.
        name (str): Name of the connection.
        password (str): Password for the connection.
        user (str): Username for the connection.
        sql_alchemy_driver (str): SQL Alchemy driver.
        engine (sqlalchemy.engine.Engine): Database engine.
    """

    db: str
    host: int
    name: str
    password: str
    user: str
    port: int
    sql_alchemy_driver: str
    engine: Engine = field(init=False, default=None)

    def __post_init__(self):
        """Initialize the connection object and create the database engine."""
        # Encode the password to be used in the connection URL
        self.password = quote_plus(self.password)
        # Create the database engine using SQLAlchemy

    def get_engine(self):
        return create_engine(
            f"{self.sql_alchemy_driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )
