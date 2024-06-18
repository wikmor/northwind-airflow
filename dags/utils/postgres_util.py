from abc import ABC, abstractmethod

from airflow.providers.postgres.hooks.postgres import PostgresHook


class BaseUtil(ABC):
    @property
    @abstractmethod
    def connection_id(self):
        """Must be overridden by subclasses to return the specific connection ID string."""
        raise NotImplementedError

    @classmethod
    def get_records(cls, sql_statement):
        return cls._get_hook().get_records(sql_statement)

    @classmethod
    def query(cls, sql_statement, parameters=None):
        cls._get_hook().run(sql_statement, parameters=parameters)

    @classmethod
    def query_multiple(cls, sql_statement, data):
        hook = cls._get_hook()
        for record in data:
            hook.run(sql_statement, parameters=record)

    @classmethod
    def _get_hook(cls):
        return PostgresHook(postgres_conn_id=cls.connection_id)


class SourceUtil(BaseUtil):
    connection_id = 'postgres_source'


class StagingUtil(BaseUtil):
    connection_id = 'postgres_staging'


class StarUtil(BaseUtil):
    connection_id = 'postgres_star'
