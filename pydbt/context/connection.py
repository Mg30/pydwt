from dataclasses import dataclass
from sqlalchemy import create_engine
from urllib.parse import quote_plus


@dataclass
class Connection(object):
    db: str
    host: int
    name: str
    password: str
    user: str
    sql_alchemy_driver: str


def __post_init__(self):
    encoded_password = quote_plus(self.password)
    self.engine = create_engine(
        f"{self.sql_alchemy_driver}://{self.user}:{encoded_password}@{self.host}:{self.port}/{self.db}"
    )
