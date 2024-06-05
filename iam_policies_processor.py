import threading
import logging
import queue
from collections import defaultdict
from google.cloud import resourcemanager_v3, bigquery
from google.oauth2 import service_account
from google.api_core import exceptions
from datetime import datetime

# Constants
CREDENTIALS_FILE = 'serviceAccount-key.json'  # Update this path
ORGANIZATION_ID = 'org_id'  # Replace with your organization ID
BQ_DATASET_NAME = 'dataset_name'  # Replace with your BigQuery dataset name
BQ_TABLE_NAME = 'table_name'  # Replace with your BigQuery table name
THREAD_COUNT = 5

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Set up GCP credentials
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)

# Initialize the Google Cloud clients
organizations_client = resourcemanager_v3.OrganizationsClient(credentials=credentials)
folders_client = resourcemanager_v3.FoldersClient(credentials=credentials)
projects_client = resourcemanager_v3.ProjectsClient(credentials=credentials)
bigquery_client = bigquery.Client(credentials=credentials)

# Initialize the Google Cloud clients
organizations_client = resourcemanager_v3.OrganizationsClient()
folders_client = resourcemanager_v3.FoldersClient()
projects_client = resourcemanager_v3.ProjectsClient()
bigquery_client = bigquery.Client()

# Create a queue to manage threads
work_queue = queue.Queue()

# Helper function to create the BigQuery dataset and table if they don't exist
def setup_bigquery_dataset_and_table():
    dataset_ref = bigquery_client.dataset(BQ_DATASET_NAME)
    table_ref = dataset_ref.table(BQ_TABLE_NAME)

    # Create dataset if it doesn't exist
    try:
        bigquery_client.get_dataset(dataset_ref)
    except bigquery.NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        bigquery_client.create_dataset(dataset)
        logger.info(f"Created dataset {BQ_DATASET_NAME}")

    # Create table if it doesn't exist
    try:
        bigquery_client.get_table(table_ref)
    except bigquery.NotFound:
        schema = [
            bigquery.SchemaField('project_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('folder_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('organization_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('member_email', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('role', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        logger.info(f"Created table {BQ_TABLE_NAME}")

# Function to insert data into BigQuery
def insert_into_bigquery(rows_to_insert):
    table_id = f"{bigquery_client.project}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logger.error(f"Encountered errors while inserting rows: {errors}")
    else:
        logger.info(f"Inserted {len(rows_to_insert)} rows into BigQuery table {table_id}")

# Function to process IAM policies and aggregate roles for a resource
def process_iam_policy(resource_name, resource_type):
    try:
        if resource_type == 'project':
            policy = projects_client.get_iam_policy(resource=resource_name)
        elif resource_type == 'folder':
            policy = folders_client.get_iam_policy(resource=resource_name)
        elif resource_type == 'organization':
            policy = organizations_client.get_iam_policy(resource=resource_name)

        member_roles = defaultdict(set)
        for binding in policy.bindings:
            for member in binding.members:
                if 'user:' in member or 'serviceAccount:' in member:
                    member_roles[member].add(binding.role)

        return [
            {
                "project_id": resource_name.split('/')[1] if resource_type == 'project' else None,
                "folder_id": resource_name.split('/')[1] if resource_type == 'folder' else None,
                "organization_id": resource_name.split('/')[1] if resource_type == 'organization' else None,
                "member_email": member,
                "role": ','.join(roles),
                "date": datetime.now().date().isoformat()
            }
            for member, roles in member_roles.items()
        ]
    except Exception as e:
        logger.error(f"Error processing {resource_type} {resource_name}: {e}")
        return []

# Worker function for threading
def worker():
    while not work_queue.empty():
        item = work_queue.get()
        if 'project_id' in item:
            resource_name = f'projects/{item["project_id"]}'
            rows_to_insert = process_iam_policy(resource_name, 'project')
        elif 'folder_id' in item:
            resource_name = f'folders/{item["folder_id"]}'
            rows_to_insert = process_iam_policy(resource_name, 'folder')
        elif 'organization_id' in item:
            resource_name = f'organizations/{item["organization_id"]}'
            rows_to_insert = process_iam_policy(resource_name, 'organization')

        if rows_to_insert:
            insert_into_bigquery(rows_to_insert)
        
        work_queue.task_done()

# Recursive function to process all folders and projects
def process_folders_and_projects(parent_id, parent_type):
    # Process current parent (folder or organization)
    if parent_type == 'organization':
        work_queue.put({'organization_id': parent_id})
    elif parent_type == 'folder':
        work_queue.put({'folder_id': parent_id})

    # Get and process all child folders
    try:
        request = resourcemanager_v3.ListFoldersRequest(parent=f"{parent_type}s/{parent_id}")
        for folder in folders_client.list_folders(request=request):
            folder_id = folder.name.split('/')[1]
            process_folders_and_projects(folder_id, 'folder')
    except exceptions.GoogleAPICallError as e:
        logger.error(f"An API error occurred while listing folders: {e}")
    except exceptions.RetryError as e:
        logger.error(f"A retryable error occurred while listing folders: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while listing folders: {e}")

    # Get and process all child projects
    try:
        request = resourcemanager_v3.SearchProjectsRequest(query=f'parent.id:{parent_id}')
        for project in projects_client.search_projects(request=request):
            project_id = project.project_id
            work_queue.put({'project_id': project_id, 'folder_id': parent_id if parent_type == 'folder' else None})
    except exceptions.GoogleAPICallError as e:
        logger.error(f"An API error occurred while listing projects: {e}")
    except exceptions.RetryError as e:
        logger.error(f"A retryable error occurred while listing projects: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while listing projects: {e}")

def main():
    setup_bigquery_dataset_and_table()

    # Start processing from the organization level
    process_folders_and_projects(ORGANIZATION_ID, 'organization')

    # Start threads
    threads = []
    for _ in range(THREAD_COUNT):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    logger.info("All projects, folders, and organization have been processed.")

if __name__ == "__main__":
    main()
