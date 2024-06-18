from base_data_transfer import BaseDataTransfer
from utils.postgres_util import StagingUtil, SourceUtil


class Transform(BaseDataTransfer):
    def __init__(self, entity: str, fetch_sql: str, clean_func, insert_sql: str):
        super().__init__(entity, fetch_sql, insert_sql)
        self.clean_func = clean_func

    def fetch_data(self):
        return SourceUtil.get_records(self.fetch_sql)

    def clean_data(self, data):
        return self.clean_func(data)

    def load_data(self, data):
        StagingUtil.query_multiple(self.insert_sql, data)
