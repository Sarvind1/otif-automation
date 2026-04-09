# OTIF Automation

A comprehensive Python system for calculating On-Time In-Full (OTIF) and Turnaround Time (TAT) metrics for supply chain operations. Integrates Redshift data sources, applies complex business logic through Excel-based mappings, and exports results to SharePoint.

## Features

- **OTIF Calculation**: Computes on-time and in-full metrics across multiple data sources
- **TAT Analysis**: Tracks turnaround time across configurable process stages with delay detection
- **Multi-Source Integration**: Combines data from Redshift, Excel mappings, and external systems
- **SharePoint Export**: Automated result uploads to SharePoint with formatted Excel files
- **Comprehensive Logging**: Detailed logs for debugging and audit trails
- **Delay Reporting**: Stage-level delay analysis with JSON and Excel exports

## Tech Stack

- **Language**: Python 3
- **Data Processing**: pandas, NumPy
- **Database**: Amazon Redshift (psycopg2)
- **Export**: openpyxl (Excel), json
- **Cloud**: SharePoint integration via Office365-REST-Python-Client
- **Utilities**: Pydantic (validation), tqdm (progress)

## Setup

### Requirements
- Python 3.8+
- Redshift database credentials
- SharePoint access
- Required packages (see `requirements.txt`)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd otif_autom
```

2. Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure credentials:
```bash
# Create creds.txt with your Redshift credentials
cat > creds.txt << EOF
user=your_redshift_user
password=your_password
database=your_database
host=your_redshift_host
port=5439
sharepoint_username=your_sp_email
sharepoint_password=your_sp_password
EOF
```

5. Update configuration:
- Modify `tat_calculator/stages_config.json` for your process stages
- Update Excel mapping files in the project root as needed

## Usage

Run the main OTIF pipeline:
```bash
python app.py
```

This executes the following steps:
1. Loads credentials from `creds.txt`
2. Ingests data from Redshift tables (PO, payments, batches, inventory)
3. Loads Excel mapping files (status, vendors, payment terms, priorities)
4. Calculates OTIF metrics and applies business rules
5. Generates Day-over-Day (DoD) analysis
6. Exports results to SharePoint

Results are saved to:
- Excel exports in `outputs/excel_exports/`
- JSON results in `outputs/tat_results/` and `outputs/delay_results/`
- CSV files in `outputs/csv_files/`
- Logs in `outputs/logs/`

### TAT Calculation

Run TAT calculation separately:
```bash
cd tat_calculator
python run_tat_calculation.py
```

Generates delay analysis, stage-level reports, and TAT export files.

## Project Structure

```
otif_autom/
├── app.py                          # Main pipeline orchestrator
├── main.py                         # OTIF calculation engine
├── ingestion_tables.py             # Redshift data ingestion
├── ingestion_excels.py             # Excel mapping loader
├── dod.py                          # Day-over-Day calculations
├── sharepoint.py                   # SharePoint client
├── static/                         # Reference data files
├── tat_calculator/                 # TAT calculation subsystem
│   ├── tat_calculator.py           # Core TAT engine
│   ├── run_tat_calculation.py      # TAT runner with reporting
│   ├── stages_config.json          # Process stage configuration
│   └── outputs/                    # Generated TAT reports
└── outputs/                        # Pipeline output files
```

## Configuration

### Stages Configuration (tat_calculator/stages_config.json)

Configure process stages, lead times, dependencies, and fallback calculations:

```json
{
  "stages": {
    "stage_name": {
      "name": "Readable Stage Name",
      "actual_timestamp": "column_name",
      "preceding_stage": "parent_stage",
      "lead_time": 5,
      "process_flow": {
        "critical_path": true,
        "team_owner": "team_name"
      }
    }
  }
}
```

## Contributing

This system integrates with multiple legacy systems. When modifying:
- Update both `ingestion_tables.py` and `ingestion_tables_multithreading.py` for schema changes
- Test Excel mappings thoroughly before production runs
- Verify Redshift SQL queries match current schema

## License

Internal use only