import json
import logging
import time
import sys
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
RETRY_DELAY = 2  # seconds

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
    Fetch data from Google Sheets with retry logic
    """
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Fetching {team_sheet['team']} - {sheet_name}, attempt {attempt + 1}/{MAX_RETRIES}")
            
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=team_sheet['sheet_id'],
                range=f"{sheet_name}!{range_value}",
                valueRenderOption='FORMATTED_VALUE'
            ).execute()
            
            return result
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                logging.warning(f"Error reading {team_sheet['team']} - {sheet_name}. Retrying in {wait_time}s. Error: {str(e)}")
                time.sleep(wait_time)
            else:
                logging.error(f"Error reading {team_sheet['team']} - {sheet_name} after {MAX_RETRIES} attempts: {str(e)}")
                raise Exception(f"Failed to read sheet after {MAX_RETRIES} attempts: {str(e)}")

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
    try:
        logging.info("Starting team capacity import")
        
        # Read config
        try:
            with open('config.json', 'r') as config_file:
                config = json.loads(config_file.read())
        except Exception as e:
            logging.error(f"Error reading config file: {str(e)}")
            raise Exception(f"Failed to read config file: {str(e)}")
        
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
            raise Exception(f"Failed to initialize credentials: {str(e)}")
        
        results = []
        
        # Process each aggregated view
        for view in team_config['aggregated_views']:
            logging.info(f"Processing view: {view['name']}")

            # If view has specific department, only process for that department
            if 'department' in view:
                departments = [view['department']]
            else:
                # Otherwise process for all departments
                departments = list(set(team['department'] for team in team_config['team_sheets']))

            for department in departments:
                logging.info(f"Processing {view['name']} for department: {department}")
                all_team_data = []
                
                # Filter teams for current department
                team_sheets = [team for team in team_config['team_sheets'] 
                             if team['department'] == department]
                
                if not team_sheets:
                    logging.warning(f"No teams found for department: {department}")
                    continue

                # Get the table name with correct prefix
                table_id = get_table_name(view, department)
                logging.info(f"Using table ID: {table_id}")
                
                # Use sheet name directly from config
                sheet_name = view['sheet_name']
                logging.info(f"Using sheet name: {sheet_name}")
                
                # Check if columns key exists in view config
                if 'columns' not in view:
                    logging.warning(f"No column mappings found for view {view['name']}. Using empty mapping.")
                    column_mappings = {}
                else:
                    column_mappings = view['columns']
                
                # Collect data from each team
                for team_sheet in team_sheets:
                    try:
                        result = fetch_sheet_with_retry(
                            sheets_service,
                            team_sheet,
                            sheet_name,
                            view['range']
                        )
                        
                        values = result.get('values', [])
                        if not values:
                            logging.warning(f"No data found for {team_sheet['team']} - {sheet_name}")
                            continue
                            
                        if len(values) > 1:  # If we have data beyond headers
                            # Get original headers
                            original_headers = values[0]
                            logging.info(f"Original headers for {team_sheet['team']} - {sheet_name}: {original_headers}")
                            
                            # Create DataFrame with original headers
                            df = pd.DataFrame(values[1:], columns=original_headers)

                            # Add metadata columns
                            df['team'] = team_sheet['team']
                            df['department'] = team_sheet['department']
                            df['import_timestamp'] = datetime.now().isoformat()

                            # Rename columns according to mapping (if any)
                            if column_mappings:
                                # Only map columns that actually exist
                                valid_mappings = {k: v for k, v in column_mappings.items() if k in df.columns}
                                df = df.rename(columns=valid_mappings)
                                
                                # Check for missing columns
                                missing_original_columns = [k for k in column_mappings.keys() if k not in df.columns]
                                if missing_original_columns:
                                    logging.warning(f"Some original columns are missing in {team_sheet['team']} - {sheet_name}: {missing_original_columns}")
                            
                            all_team_data.append(df)
                            logging.info(f"Successfully read {len(df)} rows for {team_sheet['team']} - {sheet_name}")
                        else:
                            logging.warning(f"No data rows found for {team_sheet['team']} - {sheet_name}")
                            
                    except Exception as e:
                        # Properly catch and log the exception without re-raising it
                        logging.error(f"Error processing {team_sheet['team']} - {sheet_name}: {str(e)}")
                
                if all_team_data:
                    try:
                        # Concatenate all team data
                        combined_df = pd.concat(all_team_data, ignore_index=True)
                        logging.info(f"Combined {len(combined_df)} rows for {view['name']}")
                        
                        # Convert all data to strings
                        combined_df = combined_df.astype(str)
                        
                        # Replace empty strings with None
                        combined_df = combined_df.replace(r'^\s*$', None, regex=True)
                        
                        # Prepare for upload
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        unique_id = str(uuid.uuid4())[:8]
                        gcs_filename = f"staging/team_capacity/{table_id}/{timestamp}_{unique_id}.jsonl"
                        
                        # Upload to GCS
                        logging.info(f"Uploading to GCS: {gcs_filename}")
                        bucket = storage_client.bucket(staging_bucket)
                        blob = bucket.blob(gcs_filename)
                        
                        # Convert to JSONL
                        json_data = combined_df.to_json(orient='records', lines=True, force_ascii=False)
                        blob.upload_from_string(json_data)
                        
                        gcs_uri = f"gs://{staging_bucket}/{gcs_filename}"
                        logging.info(f"Uploaded to {gcs_uri}")
                        
                        try:
                            # Get or create dataset in europe-west3
                            dataset_ref = bigquery_client.dataset(team_config['dataset_id'])
                            try:
                                dataset = bigquery_client.get_dataset(dataset_ref)
                                logging.info(f"Dataset {team_config['dataset_id']} already exists")
                            except Exception as e:
                                logging.info(f"Dataset {team_config['dataset_id']} not found, creating new one in europe-west3")
                                dataset = bigquery.Dataset(dataset_ref)
                                dataset.location = "europe-west3"
                                dataset = bigquery_client.create_dataset(dataset)
                                logging.info(f"Created dataset {team_config['dataset_id']} in europe-west3")

                            # Configure load job
                            job_config = bigquery.LoadJobConfig(
                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                autodetect=False,
                                schema=[bigquery.SchemaField(col, "STRING") for col in combined_df.columns]
                            )
                            
                            # Create table reference
                            table_ref = dataset.table(table_id)
                            
                            # Load to BigQuery (this will create a new table if it doesn't exist)
                            load_job = bigquery_client.load_table_from_uri(
                                gcs_uri,
                                table_ref,
                                job_config=job_config
                            )
                            
                            load_job.result()  # Wait for job to complete
                            
                            table = bigquery_client.get_table(table_ref)
                            result_message = (
                                f"Loaded {table.num_rows} rows for {view['name']} - {department} "
                                f"into {project_id}.{team_config['dataset_id']}.{table_id}"
                            )
                            logging.info(result_message)
                            results.append(result_message)

                        except Exception as e:
                            logging.error(f"Error in dataset/table creation: {str(e)}")
                            results.append(f"Error in dataset/table creation for {view['name']} - {department}: {str(e)}")
                    except Exception as e:
                        logging.error(f"Error processing combined data for {view['name']} - {department}: {str(e)}")
                        results.append(f"Error processing combined data for {view['name']} - {department}: {str(e)}")
                else:
                    logging.warning(f"No data collected for {view['name']} - {department}")
                    results.append(f"No data collected for {view['name']} - {department}")
            
        return "\n".join(results)
        
    except Exception as e:
        logging.error(f"Error in team capacity import: {str(e)}")
        # Return what we have so far, if anything
        if 'results' in locals() and results:
            return "\n".join(results) + f"\nEncountered error: {str(e)}"
        raise e