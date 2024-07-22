import codecs
import json
from unittest.mock import patch

import pytest
from main import app


# Helper function to encode text with rot_13
def rot13(text):
    return codecs.encode(text, "rot_13")


@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client


# Test cases for the Flask app
def test_get_sentence_success(client):
    with patch("main.get_sentence_by_id") as mock_get_sentence_by_id:
        mock_get_sentence_by_id.return_value = [{"id": "1", "text": "Hello World"}]

        response = client.get("/sentences/1")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["id"] == "1"
        assert data["text"] == "Hello World"
        assert data["cyphered_text"] == rot13("Hello World")


def test_get_sentence_invalid_id(client):
    response = client.get("/sentences/abc")

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"] == "Invalid ID supplied: id must be a positive integer"


def test_get_sentence_not_found(client):
    with patch("main.get_sentence_by_id") as mock_get_sentence_by_id:
        mock_get_sentence_by_id.return_value = []

        response = client.get("/sentences/1")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert data["error"] == "Sentence not found"


def test_add_sentence_success(client):
    with patch("main.get_sentence_by_id") as mock_get_sentence_by_id, patch(
        "main.insert_sentence"
    ) as mock_insert_sentence:
        mock_get_sentence_by_id.return_value = []
        mock_insert_sentence.return_value = []

        request_data = {"id": "2", "text": "New Sentence"}

        response = client.post("/sentences", json=request_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["id"] == "2"
        assert data["text"] == "New Sentence"
        assert data["cyphered_text"] == rot13("New Sentence")


def test_add_sentence_already_exists(client):
    with patch("main.get_sentence_by_id") as mock_get_sentence_by_id:
        mock_get_sentence_by_id.return_value = [{"id": "2", "text": "Existing Sentence"}]

        request_data = {"id": "2", "text": "New Sentence"}

        response = client.post("/sentences", json=request_data)

        assert response.status_code == 409
        data = json.loads(response.data)
        assert data["error"] == "A sentence already exists with this ID"


def test_add_sentence_invalid_input(client):
    request_data = {"id": "invalid_id", "text": 1234}

    response = client.post("/sentences", json=request_data)

    assert response.status_code == 405
    data = json.loads(response.data)
    assert data["error"] == "Invalid input: 'id' must be a positive integer string and 'text' a string."
