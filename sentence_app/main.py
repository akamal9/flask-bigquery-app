import codecs
import time

from flask import Flask, jsonify, request
from utils.bq_client import BigQueryClientSingleton
from utils.bq_operations import get_sentence_by_id, insert_sentence
from utils.bq_table_manager import check_dataset_exists, check_table_exists_and_schema, create_table

# Configure BigQuery client
db_client = BigQueryClientSingleton().client

# Define Flask app
app = Flask(__name__)


# Route for GET /sentences/{sentenceId}
@app.route("/sentences/<sentence_id>", methods=["GET"])
def get_sentence(sentence_id, client=None):

    if not sentence_id.isdigit():
        return jsonify({"error": "Invalid ID supplied: id must be a positive integer"}), 400

    try:
        rows = get_sentence_by_id(sentence_id)
    except Exception as e:
        return jsonify({"error": f"Error querying BigQuery: {str(e)}"}), 500

    # Check if sentence exists
    # rows = list(result)
    if not rows:
        return jsonify({"error": "Sentence not found"}), 404

    # Get sentence data and encrypt text
    sentence = {
        "id": rows[0]["id"],
        "text": rows[0]["text"],
        "cyphered_text": codecs.encode((rows[0]["text"]), "rot_13"),
    }

    return jsonify(sentence), 200


# Route for POST /sentences/
@app.route("/sentences", methods=["POST"])
def add_sentence(client=None):
    # Get request data (assuming JSON format)
    data = request.get_json()

    if not data or "id" not in data or "text" not in data:
        return jsonify({"error": "Invalid input: Request must contain 'id' and 'text' fields."}), 405

    if not data["id"].isdigit() or not isinstance(data["text"], str):
        return jsonify({"error": "Invalid input: 'id' must be a positive integer string and 'text' a string."}), 405

    # Prepare data for BigQuery insertion
    new_sentence = {"id": data["id"], "text": data["text"]}

    try:
        # check the id does not already exist
        if get_sentence_by_id(data["id"]):
            return jsonify({"error": "A sentence already exists with this ID"}), 409

        # Check for errors during insertion
        if insert_sentence(data["id"], data["text"]):
            return jsonify({"error": "Failed to add sentence"}), 500
    except Exception as e:
        return jsonify({"error": f"Error inserting into BigQuery: {str(e)}"}), 500

    # Encrypt the text and return the full sentence
    new_sentence = {
        "id": data["id"],
        "text": data["text"],
        "cyphered_text": codecs.encode(data["text"], "rot_13"),
    }

    return jsonify(new_sentence), 200


if __name__ == "__main__":

    if not check_dataset_exists():
        raise RuntimeError("Bigquery Dataset does not exist. Exiting script.")

    # Check if table exists, create it if not
    if not check_table_exists_and_schema() and not create_table():
        raise RuntimeError("Error in Bigquery Table creation. Exiting script.")

    app.run(debug=True)
