import json
import logging
import time
import gc
import os
import io
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
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
RETRY_DELAY_BASE = 3  # seconds (will be multiplied by 2^attempt for exponential backoff)
HTTP_TIMEOUT = 300  # 5 minutes for HTTP requests

def get_table_prefix(department):
    """
    Determines the table prefix based on department
    """
    prefix_mapping = {
        "Paid Media": "performance",
        "Paid Content": "content"
    }
    return prefix_mapping.get(department, "unknown")

def get_table_name(view, department, use_department_prefixes=True):
    """
    Constructs the table name based on view configuration and department

    Args:
        view (dict): The view configuration
        department (str): The department name
        use_department_prefixes (bool): Whether to add department prefixes to table names

    Returns:
        str: The table name with appropriate prefix (if enabled)
    """
    # If prefixes are disabled, return the base table_id as-is
    if not use_department_prefixes:
        return view['table_id']

    # Get the prefix based on department
    prefix = get_table_prefix(department)

    # Remove any existing prefix from table_id if present
    base_table_id = view['table_id'].replace('performance_', '').replace('content_', '')

    return f"{prefix}_{base_table_id}"

def export_sheet_to_gcs_with_retry(drive_service, storage_client, sheet_id, team_name, staging_bucket, group_name):
    """
    Export a Google Sheet to GCS as CSV using Drive API with retry logic.
    This is more reliable than reading via Sheets API for large datasets.

    Args:
        drive_service: Google Drive API service
        storage_client: GCS client
        sheet_id: Google Sheet ID
        team_name: Team name for logging/path
        staging_bucket: GCS bucket name
        group_name: Group name for organizing files

    Returns:
        str: GCS blob name of the exported file
    """
    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Exporting {team_name} to GCS (attempt {attempt + 1}/{MAX_RETRIES})")

            # Export sheet as CSV via Drive API
            request = drive_service.files().export_media(
                fileId=sheet_id,
                mimeType='text/csv'
            )

            # Download to memory
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logging.debug(f"Export progress: {int(status.progress() * 100)}%")

            # Generate GCS path
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_id = str(uuid.uuid4())[:8]
            gcs_blob_name = f"exports/{group_name}/{team_name}/{timestamp}_{unique_id}.csv"

            # Upload to GCS
            bucket = storage_client.bucket(staging_bucket)
            blob = bucket.blob(gcs_blob_name)
            blob.upload_from_string(fh.getvalue(), content_type='text/csv')

            gcs_uri = f"gs://{staging_bucket}/{gcs_blob_name}"
            logging.info(f"Successfully exported {team_name} to {gcs_uri}")

            return gcs_blob_name

        except HttpError as e:
            last_exception = e
            if e.resp.status in [429, 500, 502, 503, 504]:  # Retryable HTTP errors
                wait_time = RETRY_DELAY_BASE * (2 ** attempt)
                logging.warning(f"HTTP error {e.resp.status} exporting {team_name}. Retrying in {wait_time}s.")
                time.sleep(wait_time)
            else:
                logging.error(f"Non-retryable HTTP error exporting {team_name}: {str(e)}")
                raise
        except Exception as e:
            last_exception = e
            wait_time = RETRY_DELAY_BASE * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"Error exporting {team_name}. Retrying in {wait_time}s. Error: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"Error exporting {team_name} after {MAX_RETRIES} attempts: {str(e)}")

    # If we've exhausted all retries, raise the last exception
    if last_exception:
        raise last_exception

def process_team_sheet_from_gcs(storage_client, staging_bucket, gcs_blob_name, team_sheet, view, sheet_name, column_mappings):
    """
    Process a team sheet from a CSV file stored in GCS.
    This replaces the Sheets API reading approach.

    Args:
        storage_client: GCS client
        staging_bucket: GCS bucket name
        gcs_blob_name: Name of the CSV blob in GCS
        team_sheet: Team configuration dict
        view: View configuration dict
        sheet_name: Name of the sheet (for logging)
        column_mappings: Column name mappings

    Returns:
        DataFrame or None
    """
    try:
        logging.info(f"Reading {team_sheet['team']} - {sheet_name} from GCS")

        # Download CSV from GCS
        bucket = storage_client.bucket(staging_bucket)
        blob = bucket.blob(gcs_blob_name)
        csv_content = blob.download_as_text()

        # Parse CSV into DataFrame
        df = pd.read_csv(io.StringIO(csv_content))

        if df.empty:
            logging.warning(f"No data rows found for {team_sheet['team']} - {sheet_name}")
            return None

        # Add metadata columns
        df['googlesheet_name'] = f"Strang {team_sheet['team']}"
        df['department'] = team_sheet['department']
        df['import_timestamp'] = datetime.now().isoformat()

        # Rename columns according to mapping
        # Only map columns that actually exist in the dataframe
        valid_mappings = {k: v for k, v in column_mappings.items() if k in df.columns}
        df = df.rename(columns=valid_mappings)

        # Check for missing columns from the mapping
        missing_columns = set(column_mappings.keys()) - set(df.columns)
        if missing_columns:
            logging.debug(f"Some columns from mapping are missing in {team_sheet['team']} - {view['name']}: {missing_columns}")

        # Replace "nichts gefunden" and "#VALUE!" with None (NULL in the database)
        for col in df.columns:
            df[col] = df[col].replace(["nichts gefunden", "#VALUE!"], None)

        # Filter out completely empty rows (all columns are None or empty string)
        initial_row_count = len(df)
        df = df.replace(r'^\s*$', None, regex=True)  # Replace whitespace-only with None
        df = df.dropna(how='all')  # Drop rows where all columns are None

        filtered_count = initial_row_count - len(df)
        if filtered_count > 0:
            logging.info(f"Filtered out {filtered_count} empty rows for {team_sheet['team']} - {sheet_name}")

        logging.info(f"Successfully read {len(df)} rows for {team_sheet['team']} - {sheet_name}")
        return df

    except Exception as e:
        logging.error(f"Error processing {team_sheet['team']} - {sheet_name} from GCS: {str(e)}")
        return None

def fetch_sheet_with_retry(sheets_service, team_sheet, sheet_name, range_value):
    """
    Fetch data from Google Sheets with retry logic and exponential backoff
    """
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Fetching {team_sheet['team']} - {sheet_name}, attempt {attempt + 1}/{MAX_RETRIES}")
            
            # Add timeout to the request
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=team_sheet['sheet_id'],
                range=f"{sheet_name}!{range_value}",
                valueRenderOption='FORMATTED_VALUE'
            ).execute(num_retries=3)  # Built-in retries for transient errors
            
            return result
        except HttpError as e:
            last_exception = e
            if e.resp.status in [429, 500, 502, 503, 504]:  # Retryable HTTP errors
                wait_time = RETRY_DELAY_BASE * (2 ** attempt)
                logging.warning(f"HTTP error {e.resp.status} reading {team_sheet['team']} - {sheet_name}. Retrying in {wait_time}s.")
                time.sleep(wait_time)
            else:
                logging.error(f"Non-retryable HTTP error reading {team_sheet['team']} - {sheet_name}: {str(e)}")
                raise
        except Exception as e:
            last_exception = e
            wait_time = RETRY_DELAY_BASE * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"Error reading {team_sheet['team']} - {sheet_name}. Retrying in {wait_time}s. Error: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"Error reading {team_sheet['team']} - {sheet_name} after {MAX_RETRIES} attempts: {str(e)}")
    
    # If we've exhausted all retries, raise the last exception
    if last_exception:
        raise last_exception

def process_team_sheet(sheets_service, team_sheet, view, sheet_name, column_mappings):
    """Process a single team sheet and return the dataframe"""
    try:
        result = fetch_sheet_with_retry(sheets_service, team_sheet, sheet_name, view['range'])
        
        values = result.get('values', [])
        if not values or len(values) <= 1:  # No data or only headers
            logging.warning(f"No data rows found for {team_sheet['team']} - {sheet_name}")
            return None
            
        # Get original headers
        original_headers = values[0]
        
        # Create DataFrame with original headers
        df = pd.DataFrame(values[1:], columns=original_headers)

        # Add metadata columns that will help with troubleshooting
        df['googlesheet_name'] = f"Strang {team_sheet['team']}"
        df['department'] = team_sheet['department']
        df['import_timestamp'] = datetime.now().isoformat()

        # Rename columns according to mapping
        # Only map columns that actually exist in the dataframe
        valid_mappings = {k: v for k, v in column_mappings.items() if k in df.columns}
        df = df.rename(columns=valid_mappings)

        # Check for missing columns from the mapping
        missing_columns = set(column_mappings.keys()) - set(df.columns)
        if missing_columns:
            logging.debug(f"Some columns from mapping are missing in {team_sheet['team']} - {view['name']}: {missing_columns}")

        # Replace "nichts gefunden" and "#VALUE!" with None (NULL in the database)
        for col in df.columns:
            df[col] = df[col].replace(["nichts gefunden", "#VALUE!"], None)

        # Filter out completely empty rows (all columns are None or empty string)
        initial_row_count = len(df)
        df = df.replace(r'^\s*$', None, regex=True)  # Replace whitespace-only with None
        df = df.dropna(how='all')  # Drop rows where all columns are None

        filtered_count = initial_row_count - len(df)
        if filtered_count > 0:
            logging.info(f"Filtered out {filtered_count} empty rows for {team_sheet['team']} - {sheet_name}")

        logging.info(f"Successfully read {len(df)} rows for {team_sheet['team']} - {sheet_name}")
        return df
    except Exception as e:
        logging.error(f"Error processing {team_sheet['team']} - {sheet_name}: {str(e)}")
        return None

def upload_to_bigquery(df, table_id, project_id, dataset_id, storage_client, bigquery_client, staging_bucket, group_name=None):
    """Upload dataframe to BigQuery with error handling"""
    try:
        # Convert all data to strings
        df = df.astype(str)

        # Replace empty strings, "None", "nan" with None (will become NULL in BigQuery)
        df = df.replace(r'^\s*$', None, regex=True)
        df = df.replace(["None", "nan"], None)

        # Prepare for upload
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]

        # Include group name in path if provided
        path_prefix = f"staging/{group_name}/{table_id}" if group_name else f"staging/team_capacity/{table_id}"
        gcs_filename = f"{path_prefix}/{timestamp}_{unique_id}.jsonl"
        
        # Upload to GCS
        logging.info(f"Uploading to GCS: {gcs_filename}")
        bucket = storage_client.bucket(staging_bucket)
        blob = bucket.blob(gcs_filename)
        
        # Convert to JSONL
        json_data = df.to_json(orient='records', lines=True, force_ascii=False)
        blob.upload_from_string(json_data)
        
        gcs_uri = f"gs://{staging_bucket}/{gcs_filename}"
        logging.info(f"Uploaded to {gcs_uri}")
        
        # Get or create dataset in europe-west3
        dataset_ref = bigquery_client.dataset(dataset_id)
        try:
            dataset = bigquery_client.get_dataset(dataset_ref)
            logging.info(f"Dataset {dataset_id} already exists")
        except Exception as e:
            logging.info(f"Dataset {dataset_id} not found, creating new one in europe-west3")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "europe-west3"
            dataset = bigquery_client.create_dataset(dataset)
            logging.info(f"Created dataset {dataset_id} in europe-west3")

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            schema=[bigquery.SchemaField(col, "STRING") for col in df.columns]
        )
        
        # Create table reference
        table_ref = dataset.table(table_id)
        
        # Load to BigQuery (this will create a new table if it doesn't exist)
        load_job = bigquery_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        # Wait for job to complete with timeout
        load_job.result(timeout=600)  # 10 minute timeout
        
        table = bigquery_client.get_table(table_ref)
        return f"Loaded {table.num_rows} rows into {project_id}.{dataset_id}.{table_id}"
        
    except Exception as e:
        logging.error(f"Error uploading to BigQuery: {str(e)}")
        return f"Error uploading to BigQuery: {str(e)}"

def main(event, context):
    """
    Main entry point for the Cloud Function.
    Args:
        event (dict): The dictionary with data specific to this type of event.
                     For Pub/Sub, the decoded message is in event['data']
        context (google.cloud.functions.Context): The event metadata
    Returns:
        str: Operation results or error message
    """
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

def process_data_group(group_name, group_config, project_id, staging_bucket, drive_service, bigquery_client, storage_client):
    """
    Process a single data group configuration

    Args:
        group_name (str): Name of the group being processed
        group_config (dict): Configuration for this group
        project_id (str): GCP project ID
        staging_bucket (str): GCS staging bucket name
        drive_service: Google Drive API service (replaces sheets_service for better reliability)
        bigquery_client: BigQuery client
        storage_client: GCS client

    Returns:
        list: Results of processing
    """
    results = []

    try:
        logging.info(f"Processing data group: {group_name}")

        # Check if group is enabled
        if not group_config.get('enabled', True):
            logging.info(f"Group {group_name} is disabled, skipping")
            return [f"Group {group_name} is disabled"]

        # Get configuration
        use_department_prefixes = group_config.get('use_department_prefixes', True)
        dataset_id = group_config.get('dataset_id')
        team_sheets = group_config.get('team_sheets', [])
        aggregated_views = group_config.get('aggregated_views', [])

        if not dataset_id:
            error_msg = f"No dataset_id specified for group {group_name}"
            logging.error(error_msg)
            return [error_msg]

        # Process each aggregated view
        for view in aggregated_views:
            view_name = view.get('name', 'Unknown view')
            logging.info(f"Processing view: {view_name} in group {group_name}")

            # If view has specific department, only process for that department
            if 'department' in view:
                departments = [view['department']]
            else:
                # Otherwise process for all departments in this group
                departments = list(set(team['department'] for team in team_sheets))

            for department in departments:
                logging.info(f"Processing {view_name} for department: {department}")

                # Filter teams for current department
                department_team_sheets = [team for team in team_sheets
                                         if team['department'] == department]

                if not department_team_sheets:
                    logging.warning(f"No teams found for department: {department}")
                    results.append(f"No teams found for {department} in {group_name}")
                    continue

                # Get the table name with correct prefix (or no prefix)
                table_id = get_table_name(view, department, use_department_prefixes)
                logging.info(f"Using table ID: {table_id}")

                # Use sheet name directly from config
                sheet_name = view.get('sheet_name', '')
                if not sheet_name:
                    logging.error(f"No sheet name specified for view {view_name}")
                    results.append(f"No sheet name for {view_name}")
                    continue

                logging.info(f"Using sheet name: {sheet_name}")

                # Get column mappings
                column_mappings = view.get('columns', {})
                if not column_mappings:
                    logging.warning(f"No column mappings found for view {view_name}. Using empty mapping.")

                # Process each team
                all_team_data = []

                for team_sheet in department_team_sheets:
                    # Skip teams with empty sheet_ids
                    if not team_sheet.get('sheet_id') or team_sheet['sheet_id'].strip() == '':
                        logging.info(f"Skipping team {team_sheet['team']} - no sheet_id configured")
                        continue

                    try:
                        # Step 1: Export Google Sheet to GCS as CSV (more reliable than Sheets API)
                        gcs_blob_name = export_sheet_to_gcs_with_retry(
                            drive_service=drive_service,
                            storage_client=storage_client,
                            sheet_id=team_sheet['sheet_id'],
                            team_name=team_sheet['team'],
                            staging_bucket=staging_bucket,
                            group_name=group_name
                        )

                        # Step 2: Process the CSV from GCS
                        df = process_team_sheet_from_gcs(
                            storage_client=storage_client,
                            staging_bucket=staging_bucket,
                            gcs_blob_name=gcs_blob_name,
                            team_sheet=team_sheet,
                            view=view,
                            sheet_name=sheet_name,
                            column_mappings=column_mappings
                        )

                        if df is not None and not df.empty:
                            all_team_data.append(df)

                            # Force garbage collection for large dataframes
                            gc.collect()
                    except Exception as e:
                        logging.error(f"Error processing {team_sheet['team']}: {str(e)}")
                        # Continue with other teams even if one fails

                if all_team_data:
                    try:
                        # Concatenate all team data
                        combined_df = pd.concat(all_team_data, ignore_index=True)
                        logging.info(f"Combined {len(combined_df)} rows for {view_name}")

                        # Upload to BigQuery
                        upload_result = upload_to_bigquery(
                            combined_df,
                            table_id,
                            project_id,
                            dataset_id,
                            storage_client,
                            bigquery_client,
                            staging_bucket,
                            group_name=group_name
                        )
                        results.append(upload_result)

                        # Clean up memory
                        del combined_df
                        del all_team_data
                        gc.collect()

                    except Exception as e:
                        error_msg = f"Error processing combined data for {view_name} - {department}: {str(e)}"
                        logging.error(error_msg)
                        results.append(error_msg)
                else:
                    msg = f"No data collected for {view_name} - {department}"
                    logging.warning(msg)
                    results.append(msg)

        return results

    except Exception as e:
        error_msg = f"Error processing group {group_name}: {str(e)}"
        logging.error(error_msg)
        return [error_msg]


def import_team_capacity():
    """
    Core function to import data from all enabled groups or a specific group
    Reads CONFIG_GROUP environment variable to filter which group to process
    """
    all_results = []

    try:
        # Check if we should process only a specific group
        target_group = os.environ.get('CONFIG_GROUP', None)
        if target_group:
            logging.info(f"CONFIG_GROUP set to: {target_group} - will only process this group")
        else:
            logging.info("CONFIG_GROUP not set - will process all enabled groups")

        logging.info("Starting data import")

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

        # Initialize credentials
        try:
            credentials, _ = default()
            credentials.refresh(Request())

            scoped_credentials = credentials.with_scopes([
                'https://www.googleapis.com/auth/drive.readonly',  # Drive API for exporting sheets
                'https://www.googleapis.com/auth/cloud-platform'
            ])

            # Configure httplib2 with timeout for better reliability
            http = httplib2.Http(timeout=HTTP_TIMEOUT)
            http = google_auth_httplib2.AuthorizedHttp(scoped_credentials, http=http)

            # Initialize Drive API service (replaces Sheets API for more reliable exports)
            drive_service = build('drive', 'v3', credentials=scoped_credentials, http=http)
            bigquery_client = bigquery.Client(project=project_id, credentials=scoped_credentials)
            storage_client = storage.Client(project=project_id, credentials=scoped_credentials)

            logging.info(f"Initialized services with {HTTP_TIMEOUT}s HTTP timeout")
        except Exception as e:
            logging.error(f"Error initializing credentials: {str(e)}")
            return ["Failed to initialize credentials"]

        # Process each group in the config
        groups_processed = 0
        for key, value in config.items():
            # Skip non-group config items
            if key in ['project_id', 'staging_bucket'] or not isinstance(value, dict):
                continue

            # Check if this looks like a data group config
            if 'team_sheets' in value and 'aggregated_views' in value:
                # If target_group is set, only process that specific group
                if target_group and key != target_group:
                    logging.info(f"Skipping group {key} (not matching target group {target_group})")
                    continue

                logging.info(f"Processing data group: {key}")
                groups_processed += 1

                group_results = process_data_group(
                    group_name=key,
                    group_config=value,
                    project_id=project_id,
                    staging_bucket=staging_bucket,
                    drive_service=drive_service,
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

if __name__ == "__main__":
    # For local testing
    results = import_team_capacity()
    print(json.dumps(results, indent=2))