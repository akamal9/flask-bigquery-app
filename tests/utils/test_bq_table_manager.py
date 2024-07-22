from unittest.mock import MagicMock, patch

import pytest
from config import BQ_DATASET, BQ_TABLE, EXPECTED_BQ_SCHEMA
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from utils.bq_table_manager import check_dataset_exists, check_table_exists_and_schema, create_table


@pytest.fixture
def mock_bq_client():
    with patch("utils.bq_client.BigQueryClientSingleton") as MockClientSingleton:
        mock_client = MagicMock()
        MockClientSingleton.return_value.client = mock_client
        yield mock_client


def test_check_dataset_exists(mock_bq_client):
    # Arrange
    dataset_id = BQ_DATASET

    # Mock get_dataset to return a successful response
    mock_bq_client.get_dataset.return_value = MagicMock()

    # Act
    result = check_dataset_exists(dataset_id, client=mock_bq_client)

    # Assert
    assert result is True
    mock_bq_client.get_dataset.assert_called_once_with(dataset_id)


def test_check_dataset_not_exists(mock_bq_client):
    # Arrange
    dataset_id = BQ_DATASET

    # Mock get_dataset to raise NotFound exception
    mock_bq_client.get_dataset.side_effect = NotFound("Dataset not found")

    # Act
    result = check_dataset_exists(dataset_id, client=mock_bq_client)

    # Assert
    assert result is False
    mock_bq_client.get_dataset.assert_called_once_with(dataset_id)


def test_check_table_exists_and_schema(mock_bq_client):
    # Arrange
    table_id = BQ_TABLE
    table_ref = mock_bq_client.dataset().table()
    mock_table = MagicMock()
    mock_table.schema = EXPECTED_BQ_SCHEMA

    # Mock get_table to return a table with the expected schema
    mock_bq_client.get_table.return_value = mock_table

    # Act
    result = check_table_exists_and_schema(dataset_id=BQ_DATASET, table_id=table_id, client=mock_bq_client)

    # Assert
    assert result is True
    mock_bq_client.get_table.assert_called_once_with(table_ref)


def test_check_table_schema_mismatch(mock_bq_client):
    # Arrange
    table_id = BQ_TABLE
    mock_table = MagicMock()
    mock_table.schema = [bigquery.SchemaField("id", "INTEGER", mode="REQUIRED")]

    # Mock get_table to return a table with a different schema
    mock_bq_client.get_table.return_value = mock_table

    # Act & Assert
    with pytest.raises(ValueError):
        check_table_exists_and_schema(dataset_id=BQ_DATASET, table_id=table_id, client=mock_bq_client)


def test_create_table_success(mock_bq_client):
    # Arrange
    table_id = BQ_TABLE
    table_ref = mock_bq_client.dataset().table()
    table = bigquery.Table(table_ref, schema=EXPECTED_BQ_SCHEMA)

    # Mock create_table to succeed
    mock_bq_client.create_table.return_value = None

    # Act
    result = create_table(dataset_id=BQ_DATASET, table_id=table_id, schema=EXPECTED_BQ_SCHEMA, client=mock_bq_client)

    # Assert
    assert result is True
    mock_bq_client.create_table.assert_called_once_with(table)


def test_create_table_with_timeout(mock_bq_client):
    # Arrange
    table_id = BQ_TABLE
    table_ref = mock_bq_client.dataset().table()
    table = bigquery.Table(table_ref, schema=EXPECTED_BQ_SCHEMA)

    # Mock create_table to succeed and simulate waiting
    mock_bq_client.create_table.return_value = None
    mock_bq_client.get_table.side_effect = [
        NotFound("Test"),
        bigquery.Table(table_ref, schema=EXPECTED_BQ_SCHEMA),
    ]

    # Act
    result = create_table(
        dataset_id=BQ_DATASET,
        table_id=table_id,
        schema=EXPECTED_BQ_SCHEMA,
        timeout=10,
        check_interval=1,
        client=mock_bq_client,
    )

    # Assert
    assert result is True
    mock_bq_client.create_table.assert_called_once_with(table)
    assert mock_bq_client.get_table.call_count == 2


def test_create_table_with_timeout_failure(mock_bq_client):
    # Arrange
    table_id = BQ_TABLE
    table_ref = mock_bq_client.dataset().table()
    table = bigquery.Table(table_ref, schema=EXPECTED_BQ_SCHEMA)

    # Mock create_table to succeed and simulate waiting
    mock_bq_client.create_table.return_value = None
    mock_bq_client.get_table.side_effect = NotFound("Table not found")

    # Act
    result = create_table(
        dataset_id=BQ_DATASET,
        table_id=table_id,
        schema=EXPECTED_BQ_SCHEMA,
        timeout=5,
        check_interval=1,
        client=mock_bq_client,
    )

    # Assert
    assert result is False
    mock_bq_client.create_table.assert_called_once_with(table)
    assert mock_bq_client.get_table.call_count == 5
