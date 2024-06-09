import io
import json
import os
import time

import pandas as pd

from interfaces import FilesManager, Mapper, StateManager


def create_df(jsons):
    dataframes = []
    for data in jsons:
        df = pd.json_normalize(data)
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)


def to_parquet(df):
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)  # Reset buffer position to the beginning
        return buffer.getvalue()
    except Exception as e:
        raise Exception(f"Could not write DataFrame to Parquet: {e}")


def get_ts_milliseconds():
    current_time_seconds = time.time()

    current_time_milliseconds = int(current_time_seconds * 1000)
    return str(current_time_milliseconds)


def create_file_name(df, name_attr):
    current_ts = get_ts_milliseconds()
    if hasattr(df, name_attr):
        filename = f"{current_ts}-{df.attrs[name_attr]}.parquet"
    else:
        filename = f"{current_ts}.parquet"
    return filename


class Processor:
    def __init__(self, getter: FilesManager, mapper: Mapper, uploader: FilesManager, state_manager: StateManager):
        self.getter = getter
        self.mapper = mapper
        self.uploader = uploader
        self.state_manager = state_manager

    def process(self) -> None:
        state = self.state_manager.get_state()
        last_processed = ""

        if state is not None:
            last_processed = state

        try:
            files = [file for file in self.getter.list_files() if file.endswith(".json")]
        except Exception as e:
            raise Exception("could not process files: {}".format(e))

        json_data_list = []

        for file in sorted(files):
            if os.path.basename(file) <= last_processed:
                continue

            data = self.getter.download_file(file).decode("utf-8")
            json_data = json.loads(data)
            json_data_list.append(json_data)

        if len(json_data_list) > 0:
            try:
                df = create_df(json_data_list)
                transformed_objects = self.mapper.transform(df)

                for data in transformed_objects:
                    parquet_data = to_parquet(data)
                    parquet_file_name = create_file_name(data, self.mapper.NAME_ATTRIBUTE)
                    self.uploader.upload_file(parquet_file_name, parquet_data)
                self.state_manager.update_state(os.path.basename(files[-1]))
            except Exception as e:
                raise Exception("could not process files: {}".format(e))

        else:
            print("no files, or already scanned all")
