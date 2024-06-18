# from staging import StagingUtil
# from star import StarUtil
# from source import SourceUtil
#
#
# class BaseTransfer:
#     def __init__(
#             self,
#             entity: str,
#             fetch_sql: str,
#             insert_sql: str
#     ) -> None:
#         self.entity = entity
#         self.fetch_sql = fetch_sql
#         self.insert_sql = insert_sql
#
#     def transfer_data(self):
#         data = self.fetch_data()
#         processed_data = self.process_data(data)
#         StagingUtil.query_multiple(self.insert_sql, processed_data)
#
#     def fetch_data(self):
#         return SourceUtil.get_records(self.fetch_sql)
#
#     def process_data(self, data):
#         raise NotImplementedError("Subclasses should implement this method")
#
#
# class Transform(BaseTransfer):
#     def __init__(
#             self,
#             entity: str,
#             fetch_sql: str,
#             clean_func,
#             insert_sql: str
#     ) -> None:
#         super().__init__(entity, fetch_sql, insert_sql)
#         self.clean_func = clean_func
#
#     def process_data(self, data):
#         return self.clean_data(data)
#
#     def clean_data(self, data):
#         return self.clean_func(data)
#
#
# class Load(BaseTransfer):
#     def __init__(
#             self,
#             entity: str,
#             fetch_sql: str,
#             insert_sql: str
#     ) -> None:
#         super().__init__(entity, fetch_sql, insert_sql)
#
#     def process_data(self, data):
#         return data
