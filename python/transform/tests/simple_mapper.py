from abc import ABC

from pandas import DataFrame

from transform.interfaces import Mapper


class SimpleMapper(Mapper, ABC):
    def transform(self, df: DataFrame) -> DataFrame:
        return df
