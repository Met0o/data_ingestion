import os.path
import pandas as pd
import psycopg2

from psycopg2 import sql
from PyPDF2 import PdfReader
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def create_db_user_pass(host, database, user, password, port, new_db_name, new_user, new_password, superuser=False):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    cur = conn.cursor()
    conn.autocommit = True

    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (new_db_name,))
    db_exists = cur.fetchone()

    cur.execute("SELECT 1 FROM pg_catalog.pg_user WHERE usename = %s", (new_user,))
    user_exists = cur.fetchone()

    escaped_new_db_name = sql.Identifier(new_db_name)
    escaped_new_user = sql.Identifier(new_user)

    if db_exists:
        terminate_backends = sql.SQL("""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = %s;
        """)
        cur.execute(terminate_backends, (new_db_name,))

        drop_db_command = sql.SQL("DROP DATABASE IF EXISTS {}").format(escaped_new_db_name)
        cur.execute(drop_db_command)

    if user_exists:
        drop_user_command = sql.SQL("DROP USER IF EXISTS {}").format(escaped_new_user)
        cur.execute(drop_user_command)

    create_db_command = sql.SQL("CREATE DATABASE {}").format(escaped_new_db_name)
    cur.execute(create_db_command)

    create_user_command = sql.SQL("CREATE USER {} WITH PASSWORD %s {}").format(
        escaped_new_user,
        sql.SQL("SUPERUSER") if superuser else sql.SQL("")
    )
    cur.execute(create_user_command, (new_password,))

    grant_privileges_command = sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
        escaped_new_db_name,
        escaped_new_user
    )
    cur.execute(grant_privileges_command)

    conn.close()

def sheets_to_dataframe(spreadsheet_id, range_name):
    
    """Retrieves data from Google Sheets file and creates Pandas DataFrame.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets file.
        range_name (str): The name of the sheet and range to retrieve, e.g. 'Sheet1!A1:ZZ'.

    Returns:
        A Pandas DataFrame containing the data from the specified range.
    """
    
    creds = None
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    
    if os.path.exists('data/token.json'):
        creds = Credentials.from_authorized_user_file('data/token.json', SCOPES)
        
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'data/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('data/token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        return None
    else:
        num_rows = len(values)
        num_cols = max(len(row) for row in values)
        column_names = [f'Column {i}' for i in range(1, num_cols + 1)]
        
        df = pd.DataFrame(index=range(1, num_rows + 1), columns=column_names)
        
        for i, row in enumerate(values):
            for j, value in enumerate(row):
                df.iloc[i, j] = value
                
        return df

def extract_gti_data(file_path, pages):
    
    """
     Extracts data from the Global Terrorism Index PDF report.

     Args:
         pages: A list of page numbers to extract data from.

     Returns:
         A pandas dataframe containing the extracted data with the following columns:
             * GTI_Rank: The rank of the country based on the Global Terrorism Index score.
             * Country: The name of the country.
             * GTI_Score: The Global Terrorism Index score of the country based on the report issue year.
             * ChangeInScore_YoY: The change in the Global Terrorism Index score of the country tear-over-year.
     """
    
    reader = PdfReader(file_path)
    data = []
    for page_number in pages:
        page = reader.pages[page_number]
        text = page.extract_text()
        lines = text.split("\n")
        headers = [header.strip() for header in lines[2].split()]
        for line in lines[3:]:
            try:
                rank, country, score, change = line.split()
                data.append([rank, country, score, change])
            except ValueError:
                continue

    return pd.DataFrame(data, columns=['GTI_Rank', 'Country', 'GTI_Score', 'ChangeInScore_YoY'])

"""
API Ids for the WorldBank extractor. Can be adjusted as necessary to include or exclude metrics.
"""

api_ids = ['GV.POLI.ST.ES', 
           'RL.PER.RNK.UPPER', 
           'RL.PER.RNK.LOWER', 
           'PV.PER.RNK.LOWER', 
           'IC.CNS.CRIM.ZS', 
           'RL.NO.SRC', 
           'VC.IHR.PSRC.FE.P5', 
           'PV.EST', 
           'PV.PER.RNK.UPPER', 
           'GV.RULE.LW.ES', 
           'RL.STD.ERR', 
           'GCI.1STPILLAR.XQ', 
           'PV.PER.RNK', 
           'PV.STD.ERR', 
           'MO.INDEX.SRLW.XQ', 
           'RL.EST', 
           'IC.FRM.OBS.OBST6', 
           'IC.FRM.CRM.CRIME8', 
           'VC.IHR.PSRC.P5', 
           'VC.IHR.PSRC.MA.P5', 
           'RL.PER.RNK', 
           'PV.NO.SRC']