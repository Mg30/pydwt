from abc import ABC, abstractmethod
from typing import Any
import os
import dill as pickle
from dataclasses import dataclass, field


class CacheInterface(ABC):
    """Interface for caching"""

    artifact_name: str
    artifact: Any = field(init=False)

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def dump(self) -> Any:
        raise NotImplementedError


@dataclass
class LocalCache(CacheInterface):
    """Implementation of cache interface to provide a local cache
    in the form of key pair value saved in a pickle file"""

    artifact_name: str
    artifact: Any = field(init=False, default=None)

    def __post_init__(self):
        if os.path.exists(self.artifact_name):
            self.load()

    def load(self):
        with open(self.artifact_name, "rb") as cache:
            self.artifact = pickle.load(cache)

    def dump(self, artifact, path):
        with open(path, "wb") as file:
            pickle.dump(artifact, file)
