import json
import logging
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from google.auth import default
from google.auth.transport.requests import Request
import pandas as pd
from datetime import datetime
import uuid

def main(request):
    """
    Main entry point for the Cloud Function.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    start_time = datetime.now()
    logging.info(f"Starting team capacity import at {start_time}")
    
    try:
        results = import_team_capacity()
        end_time = datetime.now()
        duration = end_time - start_time
        
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
        with open('config.json', 'r') as config_file:
            config = json.loads(config_file.read())
        
        project_id = config['project_id']
        staging_bucket = config['staging_bucket']
        team_config = config['team_capacity']
        
        # Initialize clients
        credentials, project = default()
        credentials.refresh(Request())
        
        scoped_credentials = credentials.with_scopes([
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/cloud-platform'
        ])
        
        sheets_service = build('sheets', 'v4', credentials=scoped_credentials)
        bigquery_client = bigquery.Client(project=project_id, credentials=scoped_credentials)
        storage_client = storage.Client(project=project_id, credentials=scoped_credentials)
        
        results = []
        
        # Process each aggregated view
        for view in team_config['aggregated_views']:
            logging.info(f"Processing view: {view['name']}")
            all_team_data = []
            
            # Collect data from each team
            for team_sheet in team_config['team_sheets']:
                try:
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=team_sheet['sheet_id'],
                        range=f"{view['sheet_name']}!{view['range']}",
                        valueRenderOption='FORMATTED_VALUE'
                    ).execute()
                    
                    values = result.get('values', [])
                    if len(values) > 1:  # If we have data beyond headers
                        df = pd.DataFrame(values[1:], columns=values[0])
                        # Add team and department info
                        df['team'] = team_sheet['team']
                        df['department'] = team_sheet['department']
                        all_team_data.append(df)
                        
                except Exception as e:
                    logging.error(f"Error reading {team_sheet['team']} {view['name']}: {str(e)}")
            
            if all_team_data:
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
                gcs_filename = f"staging/team_capacity/{view['table_id']}/{timestamp}_{unique_id}.jsonl"
                
                # Upload to GCS
                logging.info(f"Uploading to GCS: {gcs_filename}")
                bucket = storage_client.bucket(staging_bucket)
                blob = bucket.blob(gcs_filename)
                
                # Convert to JSONL
                json_data = combined_df.to_json(orient='records', lines=True, force_ascii=False)
                blob.upload_from_string(json_data)
                
                gcs_uri = f"gs://{staging_bucket}/{gcs_filename}"
                logging.info(f"Uploaded to {gcs_uri}")
                
                # Get or create dataset
                dataset_ref = bigquery_client.dataset(team_config['dataset_id'])
                try:
                    dataset = bigquery_client.get_dataset(dataset_ref)
                except Exception:
                    dataset = bigquery_client.create_dataset(dataset_ref)
                
                # Configure load job
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    autodetect=False,
                    schema=[bigquery.SchemaField(col, "STRING") for col in combined_df.columns]
                )
                
                # Load to BigQuery
                table_ref = dataset.table(view['table_id'])
                load_job = bigquery_client.load_table_from_uri(
                    gcs_uri,
                    table_ref,
                    job_config=job_config
                )
                
                load_job.result()  # Wait for job to complete
                
                table = bigquery_client.get_table(table_ref)
                result_message = (
                    f"Loaded {table.num_rows} rows for {view['name']} "
                    f"into {project_id}.{team_config['dataset_id']}.{view['table_id']}"
                )
                logging.info(result_message)
                results.append(result_message)
            
        return "\n".join(results)
        
    except Exception as e:
        logging.error(f"Error in team capacity import: {str(e)}")
        raise e