from base_data_transfer import BaseDataTransfer
from utils.postgres_util import StagingUtil, StarUtil


class Load(BaseDataTransfer):
    def fetch_data(self):
        return StagingUtil.get_records(self.fetch_sql)

    def load_data(self, data):
        StarUtil.query_multiple(self.insert_sql, data)
