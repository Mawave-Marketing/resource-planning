import json
import logging
import time
import gc
import os
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth import default
from google.auth.transport.requests import Request
import google_auth_httplib2
import httplib2
import pandas as pd
from datetime import datetime
import uuid
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
MAX_RETRIES = 5
RETRY_DELAY_BASE = 3  # seconds
HTTP_TIMEOUT = 300  # 5 minutes for HTTP requests

def get_table_prefix(department):
    """Determines the table prefix based on department"""
    prefix_mapping = {
        "Paid Media": "performance",
        "Paid Content": "content"
    }
    return prefix_mapping.get(department, "unknown")

def get_table_name(sheet_config, department, use_department_prefixes=True):
    """Constructs the table name with appropriate prefix"""
    if not use_department_prefixes:
        return sheet_config['table_id']

    prefix = get_table_prefix(department)
    base_table_id = sheet_config['table_id'].replace('performance_', '').replace('content_', '')
    return f"{prefix}_{base_table_id}"

def fetch_sheet_with_retry(sheets_service, master_sheet_id, sheet_name, range_value):
    """Fetch data from Google Sheets with retry logic"""
    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Fetching {sheet_name}, attempt {attempt + 1}/{MAX_RETRIES}")

            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=master_sheet_id,
                range=f"{sheet_name}!{range_value}",
                valueRenderOption='FORMATTED_VALUE'
            ).execute(num_retries=3)

            return result
        except HttpError as e:
            last_exception = e
            if e.resp.status in [429, 500, 502, 503, 504]:
                wait_time = RETRY_DELAY_BASE * (2 ** attempt)
                logging.warning(f"HTTP error {e.resp.status} reading {sheet_name}. Retrying in {wait_time}s.")
                time.sleep(wait_time)
            else:
                logging.error(f"Non-retryable HTTP error reading {sheet_name}: {str(e)}")
                raise
        except Exception as e:
            last_exception = e
            wait_time = RETRY_DELAY_BASE * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"Error reading {sheet_name}. Retrying in {wait_time}s. Error: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"Error reading {sheet_name} after {MAX_RETRIES} attempts: {str(e)}")

    if last_exception:
        raise last_exception

def process_sheet(sheets_service, master_sheet_id, sheet_config, department):
    """Process a single sheet from the master Google Sheet"""
    try:
        sheet_name = sheet_config['sheet_name']
        range_value = sheet_config.get('range', 'A1:Z10000')

        result = fetch_sheet_with_retry(sheets_service, master_sheet_id, sheet_name, range_value)

        values = result.get('values', [])
        if not values or len(values) <= 1:
            logging.warning(f"No data rows found for {sheet_name}")
            return None

        # Get headers and create DataFrame
        headers = values[0]
        df = pd.DataFrame(values[1:], columns=headers)

        # Add metadata columns
        df['department'] = department
        df['import_timestamp'] = datetime.now().isoformat()

        # Rename columns according to mapping
        column_mappings = sheet_config.get('columns', {})
        valid_mappings = {k: v for k, v in column_mappings.items() if k in df.columns}
        df = df.rename(columns=valid_mappings)

        # Replace error values with None
        for col in df.columns:
            df[col] = df[col].replace(["nichts gefunden", "#VALUE!"], None)

        # Filter out empty rows
        initial_row_count = len(df)
        df = df.replace(r'^\s*$', None, regex=True)
        df = df.dropna(how='all')

        filtered_count = initial_row_count - len(df)
        if filtered_count > 0:
            logging.info(f"Filtered out {filtered_count} empty rows for {sheet_name}")

        logging.info(f"Successfully read {len(df)} rows for {sheet_name}")
        return df
    except Exception as e:
        logging.error(f"Error processing {sheet_name}: {str(e)}")
        return None

def upload_to_bigquery(df, table_id, project_id, dataset_id, storage_client, bigquery_client, staging_bucket, group_name=None):
    """Upload dataframe to BigQuery with staging through GCS"""
    try:
        # Convert all data to strings
        df = df.astype(str)
        df = df.replace(r'^\s*$', None, regex=True)
        df = df.replace(["None", "nan"], None)

        # Prepare for upload
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        path_prefix = f"staging/{group_name}/{table_id}" if group_name else f"staging/{table_id}"
        gcs_filename = f"{path_prefix}/{timestamp}_{unique_id}.jsonl"

        # Upload to GCS
        logging.info(f"Uploading to GCS: {gcs_filename}")
        bucket = storage_client.bucket(staging_bucket)
        blob = bucket.blob(gcs_filename)
        json_data = df.to_json(orient='records', lines=True, force_ascii=False)
        blob.upload_from_string(json_data)

        gcs_uri = f"gs://{staging_bucket}/{gcs_filename}"
        logging.info(f"Uploaded to {gcs_uri}")

        # Get or create dataset
        dataset_ref = bigquery_client.dataset(dataset_id)
        try:
            dataset = bigquery_client.get_dataset(dataset_ref)
        except Exception:
            logging.info(f"Creating dataset {dataset_id} in europe-west3")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "europe-west3"
            dataset = bigquery_client.create_dataset(dataset)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            schema=[bigquery.SchemaField(col, "STRING") for col in df.columns]
        )

        # Load to BigQuery
        table_ref = dataset.table(table_id)
        load_job = bigquery_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result(timeout=600)  # 10 minute timeout

        table = bigquery_client.get_table(table_ref)
        return f"Loaded {table.num_rows} rows into {project_id}.{dataset_id}.{table_id}"

    except Exception as e:
        logging.error(f"Error uploading to BigQuery: {str(e)}")
        return f"Error uploading to BigQuery: {str(e)}"

def process_data_group(group_name, group_config, project_id, staging_bucket, sheets_service, bigquery_client, storage_client):
    """Process a single data group (Kapa version)"""
    results = []

    try:
        logging.info(f"Processing data group: {group_name}")

        if not group_config.get('enabled', True):
            logging.info(f"Group {group_name} is disabled, skipping")
            return [f"Group {group_name} is disabled"]

        # Get configuration
        master_sheet_id = group_config.get('master_sheet_id')
        use_department_prefixes = group_config.get('use_department_prefixes', True)
        dataset_id = group_config.get('dataset_id')
        department_configs = group_config.get('department_configs', [])

        if not master_sheet_id:
            return [f"No master_sheet_id specified for group {group_name}"]

        if not dataset_id:
            return [f"No dataset_id specified for group {group_name}"]

        # Process each department configuration
        for dept_config in department_configs:
            department = dept_config.get('department', 'Unknown')
            sheets = dept_config.get('sheets', [])

            logging.info(f"Processing {len(sheets)} sheets for department: {department}")

            for sheet_config in sheets:
                try:
                    # Read the sheet from master
                    df = process_sheet(sheets_service, master_sheet_id, sheet_config, department)

                    if df is not None and not df.empty:
                        # Get table name with correct prefix
                        table_id = get_table_name(sheet_config, department, use_department_prefixes)
                        logging.info(f"Using table ID: {table_id}")

                        # Upload to BigQuery
                        upload_result = upload_to_bigquery(
                            df, table_id, project_id, dataset_id,
                            storage_client, bigquery_client, staging_bucket, group_name
                        )
                        results.append(upload_result)

                        # Clean up memory
                        del df
                        gc.collect()
                    else:
                        msg = f"No data for {sheet_config['sheet_name']}"
                        logging.warning(msg)
                        results.append(msg)

                except Exception as e:
                    error_msg = f"Error processing {sheet_config.get('sheet_name', 'unknown')}: {str(e)}"
                    logging.error(error_msg)
                    results.append(error_msg)

        return results

    except Exception as e:
        error_msg = f"Error processing group {group_name}: {str(e)}"
        logging.error(error_msg)
        return [error_msg]

def import_team_capacity():
    """Core function to import data from all enabled groups"""
    all_results = []

    try:
        target_group = os.environ.get('CONFIG_GROUP', None)
        if target_group:
            logging.info(f"CONFIG_GROUP set to: {target_group}")
        else:
            logging.info("CONFIG_GROUP not set - will process all enabled groups")

        # Read config
        try:
            with open('config.json', 'r') as config_file:
                config = json.loads(config_file.read())
        except Exception as e:
            logging.error(f"Error reading config file: {str(e)}")
            return ["Failed to read config file"]

        project_id = config.get('project_id')
        staging_bucket = config.get('staging_bucket')

        if not project_id or not staging_bucket:
            return ["Missing project_id or staging_bucket in config"]

        # Initialize credentials with timeout
        try:
            credentials, _ = default()
            credentials.refresh(Request())

            scoped_credentials = credentials.with_scopes([
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/cloud-platform'
            ])

            # Configure HTTP with timeout
            http = httplib2.Http(timeout=HTTP_TIMEOUT)
            http = google_auth_httplib2.AuthorizedHttp(scoped_credentials, http=http)

            # Initialize services
            sheets_service = build('sheets', 'v4', http=http)
            bigquery_client = bigquery.Client(project=project_id, credentials=scoped_credentials)
            storage_client = storage.Client(project=project_id, credentials=scoped_credentials)

            logging.info(f"Initialized services with {HTTP_TIMEOUT}s HTTP timeout")
        except Exception as e:
            logging.error(f"Error initializing credentials: {str(e)}")
            return ["Failed to initialize credentials"]

        # Process each group
        groups_processed = 0
        for key, value in config.items():
            if key in ['project_id', 'staging_bucket'] or not isinstance(value, dict):
                continue

            # Check if this is a data group with new structure
            if 'master_sheet_id' in value and 'department_configs' in value:
                if target_group and key != target_group:
                    logging.info(f"Skipping group {key} (not matching target)")
                    continue

                logging.info(f"Processing data group: {key}")
                groups_processed += 1

                group_results = process_data_group(
                    group_name=key,
                    group_config=value,
                    project_id=project_id,
                    staging_bucket=staging_bucket,
                    sheets_service=sheets_service,
                    bigquery_client=bigquery_client,
                    storage_client=storage_client
                )
                all_results.extend(group_results)

        if groups_processed == 0:
            warning_msg = f"No groups processed. Target group: {target_group if target_group else 'all'}"
            logging.warning(warning_msg)
            all_results.append(warning_msg)

        logging.info(f"Processed {groups_processed} group(s)")
        return all_results

    except Exception as e:
        error_msg = f"Error in data import: {str(e)}"
        logging.error(error_msg)
        return [error_msg]

def main(event, context):
    """Main entry point for the Cloud Function"""
    start_time = datetime.now()
    logging.info(f"Starting import job at {start_time}")

    try:
        if 'data' in event:
            pubsub_message = base64.b64decode(event['data']).decode('utf-8')
            logging.info(f"Received Pub/Sub message: {pubsub_message}")

        results = import_team_capacity()
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Import completed in {duration}")

        return json.dumps({
            'status': 'success',
            'message': results,
            'duration': str(duration)
        })

    except Exception as e:
        logging.error(f"Failed to import data: {str(e)}")
        return json.dumps({
            'status': 'error',
            'message': str(e)
        }), 500


if __name__ == "__main__":
    results = import_team_capacity()
    print(json.dumps(results, indent=2))
