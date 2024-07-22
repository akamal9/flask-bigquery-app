from unittest.mock import MagicMock, patch

import pytest
from config import BQ_DATASET, BQ_TABLE
from google.cloud.bigquery import ScalarQueryParameter
from utils.bq_operations import get_sentence_by_id, insert_sentence


@pytest.fixture
def mock_bq_client():
    with patch("utils.bq_client.BigQueryClientSingleton") as MockClientSingleton:
        mock_client = MagicMock()
        MockClientSingleton.return_value.client = mock_client
        yield mock_client


@pytest.fixture
def mock_bq_client():
    with patch("utils.bq_client.BigQueryClientSingleton") as MockClientSingleton:
        mock_client = MagicMock()
        MockClientSingleton.return_value.client = mock_client
        yield mock_client


def test_get_sentence_by_id(mock_bq_client):
    # Arrange
    sentence_id = 123
    query_result = [{"id": sentence_id, "text": "Example sentence"}]

    # Mock BigQuery job result
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = query_result
    mock_bq_client.query.return_value = mock_query_job

    # Act
    result = get_sentence_by_id(sentence_id, client=mock_bq_client)

    # Assert
    assert len(result) == len(query_result)
    assert result == query_result
    mock_bq_client.query.assert_called_once()
    assert (
        mock_bq_client.query.call_args[0][0]
        == f"""
        SELECT id, text
        FROM `{BQ_DATASET}.{BQ_TABLE}`
        WHERE id = @sentence_id
    """
    )
    assert mock_bq_client.query.call_args[1]["job_config"].query_parameters == [
        ScalarQueryParameter("sentence_id", "INTEGER", sentence_id)
    ]


def test_get_sentence_by_id_no_results(mock_bq_client):
    # Arrange
    sentence_id = 456
    query_result = []

    # Mock BigQuery job result
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = query_result
    mock_bq_client.query.return_value = mock_query_job

    # Act
    result = get_sentence_by_id(sentence_id, client=mock_bq_client)

    # Assert
    assert result == query_result
    mock_bq_client.query.assert_called_once()
    assert (
        mock_bq_client.query.call_args[0][0]
        == f"""
        SELECT id, text
        FROM `{BQ_DATASET}.{BQ_TABLE}`
        WHERE id = @sentence_id
    """
    )
    assert mock_bq_client.query.call_args[1]["job_config"].query_parameters == [
        ScalarQueryParameter("sentence_id", "INTEGER", sentence_id)
    ]


def test_insert_sentence_success(mock_bq_client):
    # Arrange
    sentence_id = 1
    sentence_text = "Test sentence"
    mock_bq_client.insert_rows_json.return_value = []

    # Act
    result = insert_sentence(sentence_id, sentence_text, client=mock_bq_client)

    # Assert
    assert result == []
    mock_bq_client.insert_rows_json.assert_called_once_with(
        f"{BQ_DATASET}.{BQ_TABLE}",
        [{"id": sentence_id, "text": sentence_text}],
        timeout=1,
    )


def test_insert_sentence_failure(mock_bq_client):
    # Arrange
    sentence_id = 1
    sentence_text = "Test sentence"
    mock_bq_client.insert_rows_json.return_value = [{"errors": "some error"}]

    # Act
    result = insert_sentence(sentence_id, sentence_text, client=mock_bq_client)

    # Assert
    assert result == [{"errors": "some error"}]
    mock_bq_client.insert_rows_json.assert_called_once_with(
        f"{BQ_DATASET}.{BQ_TABLE}",
        [{"id": sentence_id, "text": sentence_text}],
        timeout=1,
    )
