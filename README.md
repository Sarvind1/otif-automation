# OTIF Automation System

A comprehensive Python automation system for calculating On-Time In-Full (OTIF) metrics and Turnaround Time (TAT) analytics for purchase order processing workflows.

## Features

- **Redshift Data Ingestion**: Multi-threaded fetching of PO data from Redshift with error handling
- **Excel-based Mappings**: Loads vendor, status, and priority mapping files for data enrichment
- **TAT Calculator**: Configurable, stage-dependent turnaround time calculations with fallback logic
- **Day-over-Day Analysis**: Tracks OTIF metrics and delays across time periods
- **SharePoint Integration**: Automated upload of final metrics and reports to SharePoint
- **Comprehensive Logging**: Detailed logs and output organization for troubleshooting
- **Multi-threaded Processing**: Concurrent data fetching for improved performance

## Tech Stack

- **Language**: Python 3
- **Data Processing**: Pandas, NumPy
- **Database**: Amazon Redshift
- **Excel Processing**: openpyxl
- **SharePoint Integration**: Custom SharePoint client
- **Workflow**: Staged processing pipeline with dependency management

## Setup

1. **Create a virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials**:
   - Create a `creds.txt` file with your Redshift and SharePoint credentials:
     ```
     REDSHIFT_USER=your_username
     REDSHIFT_PASSWORD=your_password
     REDSHIFT_DATABASE=your_database
     REDSHIFT_HOST=your_host
     REDSHIFT_PORT=5439
     SHAREPOINT_USER=your_email
     SHAREPOINT_PASSWORD=your_password
     ```

4. **Prepare input files**:
   - Place mapping Excel files in the root directory
   - Ensure all required Redshift tables are accessible

## Usage

Run the complete OTIF pipeline:

```bash
python app.py
```

This orchestrates:
1. Credential loading
2. Redshift table ingestion
3. Excel mapping ingestion
4. Final dataframe calculation with enrichments
5. Day-over-Day view generation
6. SharePoint upload

For standalone TAT calculation:

```bash
cd tat_calculator
python run_tat_calculation.py
```

## Output

Results are organized in `outputs/`:
- `tat_results/`: JSON files with TAT calculations
- `delay_results/`: Detailed delay analysis
- `excel_exports/`: Excel reports with metrics
- `csv_files/`: Processed data exports
- `logs/`: Execution logs

## Configuration

Edit `tat_calculator/stages_config.json` to customize:
- Stage definitions and lead times
- Process flow metadata
- Dependency chains between stages
- Fallback calculation expressions