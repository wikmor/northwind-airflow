from airflow.providers.postgres.hooks.postgres import PostgresHook


class StagingUtil:
    _staging = PostgresHook(postgres_conn_id='postgres_staging')

    @classmethod
    def query_multiple(cls, sql_statement, data):
        for record in data:
            cls._staging.run(sql_statement, parameters=record)
