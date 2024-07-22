from config import BQ_PROJECT
from google.cloud import bigquery


class BigQueryClientSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BigQueryClientSingleton, cls).__new__(cls)
            cls._instance._client = bigquery.Client(project=BQ_PROJECT)
        return cls._instance

    @property
    def client(self):
        return self._instance._client
