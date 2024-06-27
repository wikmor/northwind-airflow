class BaseDataTransfer:
    def __init__(self, entity: str, fetch_sql: str, insert_sql: str):
        self.entity = entity
        self.fetch_sql = fetch_sql
        self.insert_sql = insert_sql

    def transfer_data(self):
        data = self.fetch_data()
        data = self.clean_data(data)
        self.load_data(data)

    def fetch_data(self):
        # This method should be implemented by subclasses if the fetch source differs
        raise NotImplementedError

    def clean_data(self, data):
        # Default implementation: return data without transformation
        return data

    def load_data(self, data):
        # This method should be implemented by subclasses if the load target differs
        raise NotImplementedError
