from abc import ABC
from typing import List

from pandas import DataFrame

from transform.interfaces import Mapper


class SimpleMapper(Mapper, ABC):
    def transform(self, df: DataFrame) -> List[DataFrame]:
        no_dup = df.drop_duplicates(subset='id').reset_index(drop=True)
        no_dup.attrs[self.NAME_ATTRIBUTE] = "simple_name"
        return [no_dup]
