# GCP

# IAM Policies Processor for Google Cloud

This script processes IAM policies for all projects, folders, and organizations under a specified Google Cloud Organization. It retrieves user roles and stores the information in a BigQuery table. The processing is done in parallel using multiple threads for efficiency.

## Features

- Retrieves IAM policies for Google Cloud resources.
- Processes projects, folders, and organization in parallel using threading.
- Aggregates user roles and stores them in a BigQuery table.
- Creates BigQuery dataset and table if they don't exist.

## Requirements

- Python 3.7+
- Google Cloud SDK
- Service account with appropriate permissions:
  - Access to Google Cloud Resource Manager
  - BigQuery Data Editor role

## Setup

### 1. Install Python Dependencies

```sh
pip install google-cloud-resource-manager google-cloud-bigquery
```

### 2. Configure Google Cloud SDK

Ensure that the Google Cloud SDK is installed and configured with the appropriate service account.

```sh
gcloud auth activate-service-account --key-file=path/to/service-account-file.json
gcloud config set project your-gcp-project-id
```

### 3. Service Account Key

Place your service account key file in the same directory as the script and update the `CREDENTIALS_FILE` constant in the script.

### 4. Configuration

Update the constants in the script with your details:

```python
CREDENTIALS_FILE = 'serviceAccount-key.json'  # Path to your service account key file
ORGANIZATION_ID = 'your-org-id'  # Replace with your organization ID
BQ_DATASET_NAME = 'your_dataset_name'  # Replace with your BigQuery dataset name
BQ_TABLE_NAME = 'your_table_name'  # Replace with your BigQuery table name
THREAD_COUNT = 5  # Number of threads to use
```

## Usage

Run the script using Python:

```sh
python iam_policies_processor.py
```

### Logging

The script uses Python's logging module to log information, warnings, and errors. Logs are displayed in the console with timestamps.

## Code Overview

### Functions and Classes

- `setup_bigquery_dataset_and_table()`: Creates the BigQuery dataset and table if they don't exist.
- `insert_into_bigquery(rows_to_insert)`: Inserts rows of data into the BigQuery table.
- `process_iam_policy(resource_name, resource_type)`: Processes IAM policies and aggregates roles for a resource.
- `worker()`: Worker function for threading to process items from the queue.
- `process_folders_and_projects(parent_id, parent_type)`: Recursive function to process all folders and projects under a parent resource.
- `main()`: Main function to initiate the processing and manage threads.

### Main Execution Flow

1. **Setup BigQuery**: Ensures the BigQuery dataset and table are set up.
2. **Process Organization**: Starts processing from the organization level, recursively processing all folders and projects.
3. **Threading**: Starts multiple threads to process IAM policies concurrently.
4. **Logging**: Logs the progress and any errors encountered during execution.

## Error Handling

The script includes comprehensive error handling to log and skip any issues encountered during API requests or data processing.

## Contribution

Feel free to contribute to this project by opening issues or submitting pull requests.

## License

This project is licensed under the MIT License.

---

This README provides an overview of the script's functionality, setup, and usage. Make sure to update placeholders with your actual configuration details before running the script.