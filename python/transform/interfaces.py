import io
from abc import ABC, abstractmethod
from typing import List, TypeVar, Generic, Optional
import pyarrow as pq
import pandas as pd
from pandas import DataFrame


class FilesManager(ABC):
    @abstractmethod
    def get_files(self, path: str) -> List[str]:
        """Fetch the list of file paths from the given directory or bucket."""
        pass

    @abstractmethod
    def download_file(self, file_path: str) -> str:
        """Download a file and return its content as a string."""
        pass

    @abstractmethod
    def upload_file(self, file_path: str, data: bytes) -> None:
        """Upload the given data to the specified file path."""
        pass


class Mapper(ABC):
    def __init__(self):
        self.df = None

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass




T = TypeVar('T')


class StateManager(ABC, Generic[T]):
    @abstractmethod
    def get_state(self) -> Optional[T]:
        """Get the current state, return None if no state is available."""
        pass

    @abstractmethod
    def update_state(self, state: T) -> None:
        """Update the state."""
        pass
