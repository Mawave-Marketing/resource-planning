import json
import logging
import time
import gc
from google.cloud import bigquery
from google.cloud import storage
from google.auth import default
import gspread
from gspread_dataframe import get_as_dataframe
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
import pandas as pd
from datetime import datetime
import uuid
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
MAX_RETRIES = 5
RETRY_DELAY_BASE = 3  # seconds
MAX_CONCURRENT_REQUESTS = 10  # Balance between speed and API limits
RATE_LIMIT_REQUESTS = 90  # Stay under 100 requests/100s quota
RATE_LIMIT_WINDOW = 100  # seconds

# Rate limiter: allows max concurrent requests and tracks rate limiting
rate_limiter = Semaphore(MAX_CONCURRENT_REQUESTS)

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

def parse_range(range_str):
    """
    Parse a range string like 'A1:J1000' to extract column and row limits
    Returns tuple (start_col, end_col, max_rows) or None if can't parse
    """
    try:
        parts = range_str.split(':')
        if len(parts) == 2:
            # Extract ending column and row
            end_cell = parts[1]
            # Find where letters end and numbers begin
            col_end = ''.join(c for c in end_cell if c.isalpha())
            row_end = ''.join(c for c in end_cell if c.isdigit())
            return (None, col_end, int(row_end) if row_end else None)
    except:
        pass
    return None

def fetch_sheet_with_retry(gc, team_sheet, sheet_name, range_value):
    """
    Fetch data from Google Sheets with retry logic and exponential backoff using gspread

    Args:
        gc: gspread client
        team_sheet: Team sheet configuration
        sheet_name: Name of the worksheet
        range_value: Range to fetch (e.g., 'A1:J1000')

    Returns:
        DataFrame with the sheet data
    """
    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            with rate_limiter:
                logging.info(f"Fetching {team_sheet['team']} - {sheet_name}, attempt {attempt + 1}/{MAX_RETRIES}")

                # Open spreadsheet and worksheet
                spreadsheet = gc.open_by_key(team_sheet['sheet_id'])
                worksheet = spreadsheet.worksheet(sheet_name)

                # Parse range to limit data fetching
                range_info = parse_range(range_value)

                # Fetch data as dataframe with formula evaluation
                df = get_as_dataframe(
                    worksheet,
                    evaluate_formulas=True,
                    parse_dates=False,
                    usecols=None,  # Get all columns
                    nrows=range_info[2] if range_info else None
                )

                # Clean up the dataframe
                # Remove completely empty rows and columns
                df = df.dropna(how='all', axis=0)  # Drop empty rows
                df = df.dropna(how='all', axis=1)  # Drop empty columns

                # Reset index
                df = df.reset_index(drop=True)

                logging.info(f"Successfully fetched {len(df)} rows for {team_sheet['team']} - {sheet_name}")
                return df

        except SpreadsheetNotFound:
            error_msg = f"Spreadsheet not found for {team_sheet['team']} (ID: {team_sheet['sheet_id']})"
            logging.error(error_msg)
            raise Exception(error_msg)

        except WorksheetNotFound:
            error_msg = f"Worksheet '{sheet_name}' not found in {team_sheet['team']}"
            logging.error(error_msg)
            raise Exception(error_msg)

        except APIError as e:
            last_exception = e
            # Check if it's a retryable error
            if e.response.status_code in [429, 500, 502, 503, 504]:
                # Add jitter to avoid thundering herd
                wait_time = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 1)
                logging.warning(f"API error {e.response.status_code} reading {team_sheet['team']} - {sheet_name}. Retrying in {wait_time:.2f}s.")
                time.sleep(wait_time)
            else:
                logging.error(f"Non-retryable API error reading {team_sheet['team']} - {sheet_name}: {str(e)}")
                raise

        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                # Add jitter to backoff
                wait_time = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 1)
                logging.warning(f"Error reading {team_sheet['team']} - {sheet_name}. Retrying in {wait_time:.2f}s. Error: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"Error reading {team_sheet['team']} - {sheet_name} after {MAX_RETRIES} attempts: {str(e)}")

    # If we've exhausted all retries, raise the last exception
    if last_exception:
        raise last_exception

def process_team_sheet(gc, team_sheet, view, sheet_name, column_mappings):
    """Process a single team sheet and return the dataframe"""
    try:
        df = fetch_sheet_with_retry(gc, team_sheet, sheet_name, view['range'])

        if df is None or df.empty or len(df) <= 1:
            logging.warning(f"No data rows found for {team_sheet['team']} - {sheet_name}")
            return None

        # First row should be headers - gspread-dataframe already handles this
        # But we need to handle the case where headers might be in the dataframe

        # Add metadata columns
        df['googlesheet_name'] = f"Strang {team_sheet['team']}"
        df['department'] = team_sheet['department']
        df['import_timestamp'] = datetime.now().isoformat()

        # Rename columns according to mapping
        # Only map columns that actually exist in the dataframe
        valid_mappings = {k: v for k, v in column_mappings.items() if k in df.columns}
        if valid_mappings:
            df = df.rename(columns=valid_mappings)

        # Check for missing columns from the mapping
        missing_columns = set(column_mappings.keys()) - set(df.columns)
        if missing_columns:
            logging.debug(f"Some columns from mapping are missing in {team_sheet['team']} - {view['name']}: {missing_columns}")

        # Replace "nichts gefunden" and "#VALUE!" with None (NULL in the database)
        for col in df.columns:
            df[col] = df[col].replace(["nichts gefunden", "#VALUE!"], None)

        # Filter out completely empty rows
        initial_row_count = len(df)
        df = df.replace(r'^\s*$', None, regex=True)
        df = df.dropna(how='all')

        filtered_count = initial_row_count - len(df)
        if filtered_count > 0:
            logging.info(f"Filtered out {filtered_count} empty rows for {team_sheet['team']} - {sheet_name}")

        logging.info(f"Successfully processed {len(df)} rows for {team_sheet['team']} - {sheet_name}")
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

        # Load to BigQuery
        load_job = bigquery_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )

        # Wait for job to complete with timeout
        load_job.result(timeout=600)

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

def fetch_team_sheet_concurrent(args):
    """
    Wrapper function for concurrent sheet fetching
    Returns tuple of (success, result_or_error, team_name)
    """
    gc, team_sheet, view, sheet_name, column_mappings = args
    try:
        df = process_team_sheet(gc, team_sheet, view, sheet_name, column_mappings)
        return (True, df, team_sheet['team'])
    except Exception as e:
        logging.error(f"Failed to fetch {team_sheet['team']} - {sheet_name}: {str(e)}")
        return (False, str(e), team_sheet['team'])

def process_data_group(group_name, group_config, project_id, staging_bucket, gc, bigquery_client, storage_client):
    """
    Process a single data group configuration with concurrent sheet fetching

    Args:
        group_name (str): Name of the group being processed
        group_config (dict): Configuration for this group
        project_id (str): GCP project ID
        staging_bucket (str): GCS staging bucket name
        gc: gspread client
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

                # Build list of tasks for concurrent processing
                tasks = []
                for team_sheet in department_team_sheets:
                    # Skip teams with empty sheet_ids
                    if not team_sheet.get('sheet_id') or team_sheet['sheet_id'].strip() == '':
                        logging.info(f"Skipping team {team_sheet['team']} - no sheet_id configured")
                        continue

                    tasks.append((gc, team_sheet, view, sheet_name, column_mappings))

                if not tasks:
                    logging.warning(f"No valid teams to process for {view_name} - {department}")
                    continue

                # Process sheets concurrently
                logging.info(f"Fetching {len(tasks)} sheets concurrently for {view_name} - {department}")
                all_team_data = []

                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
                    # Submit all tasks
                    future_to_team = {executor.submit(fetch_team_sheet_concurrent, task): task[1]['team']
                                     for task in tasks}

                    # Process results as they complete
                    for future in as_completed(future_to_team):
                        team_name = future_to_team[future]
                        try:
                            success, result, _ = future.result()
                            if success and result is not None and not result.empty:
                                all_team_data.append(result)
                                logging.info(f"Successfully fetched data for team {team_name}")
                            elif success:
                                logging.warning(f"No data returned for team {team_name}")
                            else:
                                logging.error(f"Failed to fetch team {team_name}: {result}")
                        except Exception as e:
                            logging.error(f"Exception processing team {team_name}: {str(e)}")

                # Upload combined data if we have any
                if all_team_data:
                    try:
                        # Concatenate all team data
                        combined_df = pd.concat(all_team_data, ignore_index=True)
                        logging.info(f"Combined {len(combined_df)} rows from {len(all_team_data)} teams for {view_name}")

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
    Core function to import data from all enabled groups
    """
    all_results = []

    try:
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

        # Initialize credentials and clients
        try:
            credentials, _ = default()

            # Initialize gspread client
            gc = gspread.authorize(credentials)

            # Initialize BigQuery and Storage clients
            bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
            storage_client = storage.Client(project=project_id, credentials=credentials)

            logging.info("Successfully initialized gspread and GCP clients")

        except Exception as e:
            logging.error(f"Error initializing clients: {str(e)}")
            return ["Failed to initialize clients"]

        # Process each group in the config
        for key, value in config.items():
            # Skip non-group config items
            if key in ['project_id', 'staging_bucket'] or not isinstance(value, dict):
                continue

            # Check if this looks like a data group config
            if 'team_sheets' in value and 'aggregated_views' in value:
                logging.info(f"Found data group: {key}")
                group_results = process_data_group(
                    group_name=key,
                    group_config=value,
                    project_id=project_id,
                    staging_bucket=staging_bucket,
                    gc=gc,
                    bigquery_client=bigquery_client,
                    storage_client=storage_client
                )
                all_results.extend(group_results)

        return all_results

    except Exception as e:
        error_msg = f"Error in data import: {str(e)}"
        logging.error(error_msg)
        return [error_msg]

if __name__ == "__main__":
    # For local testing
    results = import_team_capacity()
    print(json.dumps(results, indent=2))
