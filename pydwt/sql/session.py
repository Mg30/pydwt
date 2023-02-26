from sqlalchemy import select, Table, MetaData
from pydwt.sql.dataframe import DataFrame


class Session:
    def __init__(self, engine, schema: str = None):
        """Initialize a new Session object.

        Args:
            engine: A SQLAlchemy engine object.
            schema (str): The schema of the database.
        """
        self._engine = engine
        if schema:
            self._metadata = MetaData(schema=schema)
        else:
            self._metadata = MetaData()

    def table(self, name: str) -> DataFrame:
        """Create a new DataFrame object from the given table name.

        Args:
            name (str): The name of the table.

        Returns:
            DataFrame: A new DataFrame object.
        """
        t = Table(name, self._metadata, autoload_with=self._engine)
        base = select(t).cte()
        return DataFrame(base, self._engine)
