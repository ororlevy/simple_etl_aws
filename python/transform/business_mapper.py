import re

import pandas as pd
from interfaces import Mapper
from typing import List


class BusinessMapper(Mapper):
    def __init__(self, user_file_name: str, company_file_name: str):
        super().__init__()
        self.user_file_name = user_file_name
        self.company_file_name = company_file_name

    def transform(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        # Remove duplicates by 'id'
        df = df.drop_duplicates(subset='id')

        # Remove records where the 'email' field is missing or invalid
        df = df[df['email'].notna() & df['email'].apply(self.is_valid_email)]

        # Convert the 'id' to a string format
        df['id'] = df['id'].astype(str)

        # Create a new field 'domain' extracted from the 'email' address
        df['domain'] = df['email'].apply(lambda x: x.split('@')[1])

        # Add a new field 'full_address' concatenating 'street', 'suite', 'city', and 'zipcode'
        df['full_address'] = df.apply(lambda
                                          x: f"{x['address']['street']}, {x['address']['suite']}, {x['address']['city']}, {x['address']['zipcode']}",
                                      axis=1)

        # Filter out records where the 'username' contains less than 5 characters
        df = df[df['username'].str.len() >= 5]

        df.attrs[Mapper.NAME_ATTRIBUTE] = self.user_file_name

        grouped_df = df.groupby('company.name').size().reset_index(name='user_count')
        grouped_df.attrs[Mapper.NAME_ATTRIBUTE] = self.company_file_name

        return [df, grouped_df]

    @staticmethod
    def is_valid_email(email: str) -> bool:
        # Regular expression to validate an email address
        email_regex = re.compile(
            r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        )
        return re.match(email_regex, email) is not None
