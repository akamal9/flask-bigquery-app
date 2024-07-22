from google.cloud import bigquery

BQ_PROJECT = "vp-cloud-data-sandbox"
BQ_DATASET = "akamal"
BQ_TABLE = "sentences"
EXPECTED_BQ_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
]
