# Flask BigQuery App

This is a Flask application that provides an API for interacting with a Google BigQuery dataset. The application allows you to add and retrieve sentences stored in BigQuery, with text encryption using ROT13.

## Table of Contents

- [Installation](#installation)
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
- [Testing](#testing)
- [Docker](#docker)


## Installation

### Prerequisites

- Python 3.11+
- pip (Python package installer)
- Google Cloud SDK (for authentication)

### Clone the Repository

First, clone the repository from GitHub:

```sh
git clone https://github.com/yourusername/flask-bigquery-app.git
cd flask-bigquery-app
```

### Create and Activate a Virtual Environment
It's a good practice to use a virtual environment to manage your project dependencies. Create and activate a virtual environment using the following commands:

```sh
python3 -m venv venv
source venv/bin/activate
```

### Install Dependencies
Once the virtual environment is activated, install the required Python packages:

```sh
pip install -r requirements.txt
```

### Google Cloud Authentication
Ensure that you have the Google Cloud SDK installed. Authenticate with Google Cloud to allow the application to access BigQuery:

```sh
gcloud auth application-default login
```

### Set Up Google Cloud BigQuery
Create a Google Cloud project if you don't have one already.
Enable the BigQuery API for your project.
Create a BigQuery dataset and table in your project. Modify the config.py file in the project root to reflect your dataset and table names.
Configuration
Environment Variables
Create a .env file in the root of the project and add your Google Cloud project ID:

```plaintext
GOOGLE_CLOUD_PROJECT=your-google-cloud-project-id
```

# Running the Application
Create BigQuery Dataset and Table
Ensure your BigQuery dataset and table exist. Modify the config.py file to reflect your dataset and table names.

### Start the Flask Application
Start the Flask development server by running:

```sh
python sentence_app/main.py
```

The application will be available at http://127.0.0.1:5000.

# API Endpoints
## GET /sentences/<sentence_id>
Retrieve a sentence by its ID.

- **URL: /sentences/<sentence_id>**
- **Method: GET**
- **Success Response**:
  - **Code**: 200
  - **Content**: { "id": "1", "text": "Hello World", "cyphered_text": "Uryyb Jbeyq" }
- **Error Responses**:
  - **400**: Invalid ID supplied
  - **404**: Sentence not found
## POST /sentences
Add a new sentence.

- **URL: /sentences**
- **Method: POST**
- **Payload:**
  - **Content-Type**: application/json
  - **Body**: { "id": "2", "text": "New Sentence" }
- **Success Response:**
  - **Code**: 200
  - **Content**: { "id": "2", "text": "New Sentence", "cyphered_text": "Arj Frafrapr" }
- **Error Responses**:
    - **405**: Invalid input
    - **409**: A sentence already exists with this ID
    - **500**: Error inserting into BigQuery
# Testing
## Running Unit Tests
The application uses pytest for testing. To run the tests:

```sh
pytest tests/
```
## Test Configuration
The tests are located in the tests/ directory and use fixtures and mocks to simulate BigQuery interactions.

# Docker
### Building the Docker Image
```sh
docker build -t flask-bigquery-app .
```

### Running the Docker Container
```sh
docker run -p 5000:5000 --env-file .env flask-bigquery-app
```
