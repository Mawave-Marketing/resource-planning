import json
import logging
import time
import gc
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth import default
from google.auth.transport.requests import Request
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
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # seconds (will be multiplied by 2^attempt for exponential backoff)

def get_table_prefix(department):
    """
    Determines the table prefix based on department
    """
    prefix_mapping = {
        "Paid Media": "performance",
        "Paid Content": "content"
    }
    return prefix_mapping.get(department, "unknown")

def get_table_name(view, department):
    """
    Constructs the table name based on view configuration and department
    
    Args:
        view (dict): The view configuration
        department (str): The department name
    
    Returns:
        str: The table name with appropriate prefix
    """
    # Get the prefix based on department
    prefix = get_table_prefix(department)
    
    # Remove any existing prefix from table_id if present
    base_table_id = view['table_id'].replace('performance_', '').replace('content_', '')
    
    return f"{prefix}_{base_table_id}"

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
            ).execute(num_retries=2)  # Built-in retries for transient errors
            
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
        df['team'] = team_sheet['team']
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
        
        logging.info(f"Successfully read {len(df)} rows for {team_sheet['team']} - {sheet_name}")
        return df
    except Exception as e:
        logging.error(f"Error processing {team_sheet['team']} - {sheet_name}: {str(e)}")
        return None

def upload_to_bigquery(df, table_id, project_id, dataset_id, storage_client, bigquery_client, staging_bucket):
    """Upload dataframe to BigQuery with error handling"""
    try:
        # Convert all data to strings
        df = df.astype(str)
        
        # Replace empty strings with None
        df = df.replace(r'^\s*$', None, regex=True)
        
        # Prepare for upload
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        gcs_filename = f"staging/team_capacity/{table_id}/{timestamp}_{unique_id}.jsonl"
        
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

def import_team_capacity():
    """
    Core function to import and concatenate team capacity data
    """
    results = []
    
    try:
        logging.info("Starting team capacity import")
        
        # Read config
        try:
            with open('config.json', 'r') as config_file:
                config = json.loads(config_file.read())
        except Exception as e:
            logging.error(f"Error reading config file: {str(e)}")
            return ["Failed to read config file"]
        
        project_id = config['project_id']
        staging_bucket = config['staging_bucket']
        team_config = config['team_capacity']
        
        # Initialize credentials
        try:
            credentials, project = default()
            credentials.refresh(Request())
            
            scoped_credentials = credentials.with_scopes([
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/cloud-platform'
            ])
            
            sheets_service = build('sheets', 'v4', credentials=scoped_credentials)
            bigquery_client = bigquery.Client(project=project_id, credentials=scoped_credentials)
            storage_client = storage.Client(project=project_id, credentials=scoped_credentials)
        except Exception as e:
            logging.error(f"Error initializing credentials: {str(e)}")
            return ["Failed to initialize credentials"]
        
        # Process each aggregated view
        for view in team_config['aggregated_views']:
            view_name = view.get('name', 'Unknown view')
            logging.info(f"Processing view: {view_name}")

            # If view has specific department, only process for that department
            if 'department' in view:
                departments = [view['department']]
            else:
                # Otherwise process for all departments
                departments = list(set(team['department'] for team in team_config['team_sheets']))

            for department in departments:
                logging.info(f"Processing {view_name} for department: {department}")
                
                # Filter teams for current department
                team_sheets = [team for team in team_config['team_sheets'] 
                             if team['department'] == department]
                
                if not team_sheets:
                    logging.warning(f"No teams found for department: {department}")
                    results.append(f"No teams found for {department}")
                    continue

                # Get the table name with correct prefix
                table_id = get_table_name(view, department)
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
                
                for team_sheet in team_sheets:
                    try:
                        df = process_team_sheet(
                            sheets_service, 
                            team_sheet, 
                            view, 
                            sheet_name, 
                            column_mappings
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
                            team_config['dataset_id'], 
                            storage_client, 
                            bigquery_client, 
                            staging_bucket
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
        error_msg = f"Error in team capacity import: {str(e)}"
        logging.error(error_msg)
        results.append(error_msg)
        return results