import io
import json
import os
import time

import pandas as pd
import pyarrow as pq

from .interfaces import FilesManager, Mapper, StateManager


def create_df(jsons):
    dataframes = []
    for data in jsons:
        df = pd.json_normalize(data)
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)


def to_parquet(df):
    df = df.drop_duplicates(subset='id').reset_index(drop=True)
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)  # Reset buffer position to the beginning
        return buffer.getvalue()
    except Exception as e:
        raise Exception(f"Could not write DataFrame to Parquet: {e}")


def get_file_name():
    current_time_seconds = time.time()

    current_time_milliseconds = int(current_time_seconds * 1000)
    return str(current_time_milliseconds)


class Processor:
    def __init__(self, getter: FilesManager, mapper: Mapper, uploader: FilesManager, state_manager: StateManager[str]):
        self.getter = getter
        self.mapper = mapper
        self.uploader = uploader
        self.state_manager = state_manager

    def process(self, input_path: str, output_path: str) -> None:
        state = self.state_manager.get_state()
        last_processed = ""

        if state is not None:
            last_processed = state

        try:
            files = [file for file in self.getter.get_files(input_path) if file.endswith(".json")]
        except Exception as e:
            raise Exception("could not process files: {}".format(e))

        json_data_list = []

        for file in sorted(files):
            if os.path.basename(file) <= last_processed:
                continue

            data = self.getter.download_file(file)
            json_data = json.loads(data)
            json_data_list.append(json_data)

        if len(json_data_list) > 0:
            try:
                df = create_df(json_data_list)
                transformed = self.mapper.transform(df)

                parquet_data = to_parquet(transformed)

                parquet_file_name = f'{get_file_name()}.parquet'
                parquet_file_path = os.path.join(output_path, parquet_file_name)
                self.uploader.upload_file(parquet_file_path, parquet_data)
                self.state_manager.update_state(os.path.basename(files[-1]))
            except Exception as e:
                raise Exception("could not process files: {}".format(e))

        else:
            print("no files, or already scanned all")
