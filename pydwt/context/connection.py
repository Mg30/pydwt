from dataclasses import dataclass, field
from typing import Dict
from sqlalchemy import create_engine, Engine


@dataclass
class Connection(object):
    """Class representing a connection to a database.

    Attributes:
        params (Dict): containning SQL alchemy DB url and Kwargs forwarded to create_engine
        engine (sqlalchemy.engine.Engine): Database engine.
    """

    params: Dict
    engine: Engine = field(init=False, default=None)

    def get_engine(self):
        return create_engine(**self.params)
