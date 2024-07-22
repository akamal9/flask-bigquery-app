import argparse
import gzip
import json
import logging
import os
import signal
import subprocess
import time

import requests
from utils.bq_table_manager import check_dataset_exists, check_table_exists_and_schema

FLASK_APP_PATH = "./main.py"
FLASK_HOST = "127.0.0.1"
FLASK_PORT = 5000
API_URL = f"http://{FLASK_HOST}:{FLASK_PORT}/sentences"

DOWNLOAD_URL = "https://storage.googleapis.com/tempspace_eu_regional/fsi_test/sentences.json.gz"
LOCAL_FILE = "sentences.json.gz"


def start_flask_app(flask_app_path, host, port):
    """Start the Flask API server."""
    return subprocess.Popen(["python", flask_app_path, "--host", host, "--port", str(port)])


def stop_flask_app(process):
    """Stop the Flask API server."""
    os.kill(process.pid, signal.SIGTERM)


def download_file(url, local_filename):
    """Download file from a URL."""
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    logging.info(f"Downloaded file: {local_filename}")


def load_sentences(file_name, lines_limit):
    """Load sentences from a file and send them to the API."""
    response_times = []
    lines_processed = 0

    with gzip.open(file_name, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    sentence = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

                start_time = time.time()
                response = requests.post(API_URL, json=sentence)
                end_time = time.time()

                response_times.append(end_time - start_time)

                if response.status_code != 200:
                    logging.error(f"Failed to load sentence: {response.text}")
                else:
                    logging.info(f"Loaded sentence: {response.json()}")

                lines_processed += 1
                if lines_limit and lines_processed >= lines_limit:
                    break

    return response_times


def analyze_performance(response_times):
    """Analyze and print performance metrics."""
    total_time = sum(response_times)
    average_time = total_time / len(response_times)
    logging.info(f"Total time: {total_time:.2f} seconds")
    logging.info(f"Minimum time per request: {min(response_times):.2f} seconds")
    logging.info(f"Average time per request: {average_time:.2f} seconds")
    logging.info(f"Maximum time per request: {max(response_times):.2f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate BigQuery table with sentences.")
    parser.add_argument(
        "--lines",
        type=int,
        default=None,
        help="Limit the number of lines to process from the file.",
    )
    args = parser.parse_args()

    try:
        # Start the Flask API
        flask_process = start_flask_app(FLASK_APP_PATH, FLASK_HOST, FLASK_PORT)
        time.sleep(5)  # Wait a few seconds to ensure the Flask server is up
        # Check if dataset exists, create if not
        if not check_dataset_exists():
            raise RuntimeError("Bigquery Dataset does not exist. Exiting script.")

        # Check if table exists, create it if not
        if not check_table_exists_and_schema():
            raise RuntimeError("BigQuery Table does not exist. Exiting script.")

        # Download the file
        download_file(DOWNLOAD_URL, LOCAL_FILE)

        # Load sentences and measure performance
        response_times = load_sentences(LOCAL_FILE, args.lines)
        analyze_performance(response_times)

    finally:
        # Clean up
        if os.path.exists(LOCAL_FILE):
            os.remove(LOCAL_FILE)
            logging.info(f"Removed file: {LOCAL_FILE}")
        # Stop the Flask API
        stop_flask_app(flask_process)
