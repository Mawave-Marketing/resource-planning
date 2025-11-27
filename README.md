# Resource Planning Data Import System

## Overview

This system automatically imports resource planning data from Google Sheets into BigQuery tables. It supports multiple data groups (e.g., Kapa 1.0, Kapa 2.0) with different team configurations and departments.

## Architecture

```
Google Sheets (Multiple Teams)
    → Python Script (main.py)
    → Google Cloud Storage (Staging)
    → BigQuery Tables
```

### Components

- **config.json**: Configuration file defining data groups, teams, sheets, and column mappings
- **main.py**: Python script that orchestrates the data import process
- **Google Cloud Function**: Triggered by Pub/Sub messages or manual execution
- **Google Cloud Storage**: Temporary staging area for JSONL files
- **BigQuery**: Final destination for aggregated data

## Configuration Structure

### Data Groups

Each data group (e.g., `kapa_1_0`, `kapa_2_0`) contains:

```json
{
  "enabled": true,
  "group_name": "Kapa 2.0",
  "use_department_prefixes": false,
  "team_sheets": [...],
  "aggregated_views": [...],
  "dataset_id": "dl_resource_planning"
}
```

#### Key Configuration Options

- **enabled**: Whether this group should be processed
- **group_name**: Display name for the group
- **use_department_prefixes**: Whether to add department prefixes to table names (e.g., `performance_`, `content_`)
- **team_sheets**: Array of team configurations with Google Sheet IDs
- **aggregated_views**: Array of view configurations defining what data to extract
- **dataset_id**: BigQuery dataset where tables will be created

### Team Sheets

Each team entry contains:
```json
{
  "team": "CB",
  "sheet_id": "1wNb_AoJ_aA09e6YqG63srQdOhzk-u0_2QmDAWEksjQE",
  "department": "Paid Content"
}
```

### Aggregated Views

Each view defines what data to extract and how to map columns:

```json
{
  "name": "Client_Level",
  "sheet_name": "Concatenated_Client_Level",
  "range": "A1:J1000",
  "table_id": "kapa2_client_level",
  "columns": {
    "Year_Month": "year_month",
    "Projekt": "projekt",
    "Client": "client"
  }
}
```

- **name**: Display name for the view
- **sheet_name**: Name of the sheet tab in Google Sheets
- **range**: Cell range to import (e.g., "A1:J1000")
- **table_id**: BigQuery table name (may be prefixed based on `use_department_prefixes`)
- **columns**: Mapping from Google Sheet column names (left) to BigQuery column names (right)
- **department** (optional): Specific department filter for this view

## Data Flow

### 1. Sheet Reading
- Script reads each team's Google Sheet using the Sheets API
- Retries with exponential backoff on transient errors (429, 500, 502, 503, 504)
- Filters out empty rows and replaces error values (`"nichts gefunden"`, `"#VALUE!"`) with NULL

### 2. Data Processing
- Adds metadata columns: `googlesheet_name`, `department`, `import_timestamp`
- Renames columns according to the mapping in config.json
- Concatenates data from all teams in the same department

### 3. Upload to BigQuery
- Converts DataFrame to JSONL format
- Uploads to Google Cloud Storage as staging file
- Loads from GCS to BigQuery using **WRITE_TRUNCATE** disposition
- All columns are created as STRING type

### 4. Table Management

**Important**: The script uses `WRITE_TRUNCATE` mode, which means:
- Tables are completely replaced on each run
- All existing data is deleted before new data is loaded
- Table schema is regenerated from the DataFrame
- You don't need to manually drop tables

## Adding New Teams/Departments

### Example: Adding Organic Social Teams

1. Add team entries to `team_sheets` in the appropriate data group:

```json
{
  "team": "OA",
  "sheet_id": "YOUR_SHEET_ID_HERE",
  "department": "Organic Social"
}
```

2. The script will automatically:
   - Process these teams during the next run
   - Combine data from all Organic Social teams
   - Create/update BigQuery tables accordingly

## Column Mapping

### How It Works

Column mappings in `config.json` define the transformation:

```json
"columns": {
  "Product_Index": "product_index"
}
```

- **Left side** ("Product_Index"): Must match the **exact column name** in your Google Sheet
- **Right side** ("product_index"): Becomes the **column name in BigQuery**

### Changing Column Names

When you change a column mapping:
1. The new column name will appear in BigQuery
2. The old column name will be gone (due to WRITE_TRUNCATE)
3. **Update any queries or dashboards** that reference the old column name

### Missing Columns

If a column in the mapping doesn't exist in the sheet:
- It will be logged as a warning
- The script will continue processing other columns
- No error will occur

## Departments and Prefixes

### Department Prefix Mapping (Kapa 1.0)

When `use_department_prefixes: true`:
- Paid Media → `performance_` prefix
- Paid Content → `content_` prefix

Example: `aggregated_monitoring` → `performance_aggregated_monitoring`

### No Prefixes (Kapa 2.0)

When `use_department_prefixes: false`:
- Table IDs are used as-is
- Example: `kapa2_client_level` stays as `kapa2_client_level`

## Running the Script

### Local Testing
```bash
cd src
python main.py
```

### Cloud Function Deployment
The function is triggered by Pub/Sub messages and runs automatically on schedule.

## Monitoring and Logging

The script provides detailed logging:
- Sheet reading attempts and retries
- Row counts for each team
- Empty row filtering
- Upload progress to GCS and BigQuery
- Error messages with context

Check Cloud Function logs for execution details.

## Common Issues

### Empty Sheet IDs
If a team has an empty `sheet_id` (""), the script will attempt to read it and fail. Fill in valid Sheet IDs before running.

### Permission Errors
Ensure the service account has:
- Google Sheets API read access to all sheets
- BigQuery data editor role
- Cloud Storage object creator role

### Rate Limiting
The script includes retry logic for rate limiting (HTTP 429). If you hit limits consistently, consider:
- Reducing the number of concurrent reads
- Increasing retry delays
- Requesting quota increases

## BigQuery Schema

All columns are imported as STRING type. You can:
- Create views with CAST operations for proper types
- Use BigQuery's schema evolution for type changes
- Parse dates/numbers in downstream queries

## Best Practices

1. **Test configuration changes locally** before deploying
2. **Document any custom column mappings** in team communications
3. **Update dependent queries** when changing column names
4. **Monitor execution logs** for warnings about missing columns
5. **Keep sheet ranges generous** (e.g., A1:J1000) to avoid truncation
6. **Use consistent naming conventions** across sheets and departments

## Support

For issues or questions about this system, check:
- Cloud Function logs in GCP Console
- Script output when running locally
- This README for configuration guidance
