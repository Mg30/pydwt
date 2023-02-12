from dataclasses import dataclass
from pydbt.sql.session import Session
from typing import Any

@dataclass
class Datasources(object):
    referentiel: dict
    engine: Any

    def get_source(self, name: str):
        config = self.referentiel[name]
        session = Session(engine=self.engine, schema=config["schema"])
        return session.table(config["table"])
