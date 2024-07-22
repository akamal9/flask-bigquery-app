import logging
import time

from config import BQ_DATASET, BQ_TABLE, EXPECTED_BQ_SCHEMA
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from utils.bq_client import BigQueryClientSingleton


def check_dataset_exists(dataset_id=BQ_DATASET, client=None):
    """Check if the BigQuery dataset exists."""
    try:
        db_client = client or BigQueryClientSingleton().client
        db_client.get_dataset(dataset_id)
        logging.info(f"Dataset {dataset_id} exists.")
        return True
    except NotFound:
        logging.info(f"Dataset {dataset_id} does not exist.")
        return False


def check_table_exists_and_schema(
    dataset_id=BQ_DATASET,
    table_id=BQ_TABLE,
    expected_schema=EXPECTED_BQ_SCHEMA,
    client=None,
):
    """Check if the BigQuery table exists and has the expected schema."""
    db_client = client or BigQueryClientSingleton().client
    table_ref = db_client.dataset(dataset_id).table(table_id)
    try:
        # Check if the schema matches the expected schema
        actual_schema = db_client.get_table(table_ref).schema

        if actual_schema != expected_schema:
            logging.fatal(f"Table {table_id} has a different schema.")
            raise ValueError(f"Table {table_id} has a different schema.")
        return True
    except NotFound:
        logging.warn(f"Table {table_id} does not exist.")
        return False


def create_table(
    dataset_id=BQ_DATASET,
    table_id=BQ_TABLE,
    schema=EXPECTED_BQ_SCHEMA,
    client=None,
    timeout=60,  # Timeout in seconds to wait for table creation
    check_interval=5,  # Interval in seconds between existence checks
):
    """Create the BigQuery table with the expected schema and wait for confirmation."""
    db_client = client or BigQueryClientSingleton().client
    table_ref = db_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    try:
        # Create the table
        db_client.create_table(table)
        logging.info(f"Started creating table {table_id} with schema {schema}.")

        # Poll to check if the table is created
        elapsed_time = 0
        while elapsed_time < timeout:
            try:
                # Try to get the table to confirm creation
                db_client.get_table(table_ref)
                logging.info(f"Table {table_id} has been created successfully.")
                return True
            except NotFound:
                # Table not found, wait and retry
                logging.info(f"Waiting for the creation of Table {table_id} ...")
                time.sleep(check_interval)
                elapsed_time += check_interval

        # If timeout is reached
        logging.fatal(f"Table {table_id} was not created within the timeout period.")
        return False
    except Exception as e:
        logging.fatal(f"Error creating table {table_id}: {str(e)}")
        return False
