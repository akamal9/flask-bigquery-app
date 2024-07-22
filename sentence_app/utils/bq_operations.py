from config import BQ_DATASET, BQ_TABLE
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery.query import ScalarQueryParameter
from utils.bq_client import BigQueryClientSingleton


def get_sentence_by_id(sentence_id, client=None):
    """
    Retrieve a sentence from BigQuery by its ID.

    Args:
        sentence_id (int): The ID of the sentence to retrieve.

    Returns:
        list: A List containing the sentence data or an error response.
    """
    db_client = client or BigQueryClientSingleton().client

    # Build BigQuery query
    query = f"""
        SELECT id, text
        FROM `{BQ_DATASET}.{BQ_TABLE}`
        WHERE id = @sentence_id
    """

    # Set up the query job configuration with parameters
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("sentence_id", "INTEGER", sentence_id),
        ]
    )
    # Execute the query
    query_job = db_client.query(query, job_config=job_config, timeout=1)
    return list(query_job.result())


def insert_sentence(id, text, client=None):
    # Insert new sentence into BigQuery table
    data = {"id": id, "text": text}
    db_client = client or BigQueryClientSingleton().client
    return db_client.insert_rows_json(f"{BQ_DATASET}.{BQ_TABLE}", [data], timeout=1)
