from sqlalchemy import select, Table, MetaData
from pydbt.sql.dataframe import DataFrame

class Session(object):
    def __init__(self, engine, schema):
        self._engine = engine
        self._metadata = MetaData(schema=schema)

    def table(self, name)-> DataFrame:
        t = Table(name, self._metadata, autoload_with=self._engine)
        base = select(t).cte()
        return DataFrame(base, self._engine)