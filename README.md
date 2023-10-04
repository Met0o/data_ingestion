Postgres database deployment and configuration codebase

## Prerequisites

- Python 3.9.16
- Anaconda (optional)
- PostgreSQL server
- Required Python packages should be installed with one of the following:
  - `pip install -r requirements.txt`
  - `conda env create -f environment.yml`

## Configuration

1. Update the `db_config.py` file with the appropriate database credentials and other configurations required for the DB deployment.

## Deployment

To trigger the deployment process to a remote DB, open a terminal and navigate to the project directory and run:

  * python applicator.py

To trigger the deployment as part of a docker container locally run:

  * docker-compose build
  * docker-compose up

This script will:

1. Create a new database and user (if needed).
2. Load data from various sources (CSV, XLSX, PDF, APIs, and Google Sheets) into the specified PostgreSQL database.
   
   * NB - The deployment pipeline will attempt to connect to a Google Sheets file to source project artifacts. 
   * NB2 - Knoema API has daily limits of 10000 tokens, which results in 3 full runs of the script per day. Limits are reset after 12 hours.
   
3. Execute a series of SQL scripts to normalize, optimize, and organize the data within the database.

## Troubleshooting

If you encounter any issues or errors during the script execution, check the following:

1. Review the app.log file for any error messages which will be generated after each run in the root directory.
2. Ensure that the PostgreSQL server is running and properly configured.
2. Verify that the provided credentials in `db_config.py` are correct.