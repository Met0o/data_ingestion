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

import os.path
import logging
import pandas as pd
import requests
import psycopg2
import knoema

from psycopg2.extras import execute_values
from tqdm import tqdm

from utils import sheets_to_dataframe, extract_gti_data

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

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


logging.info(f"Loading data from PDF file: {file_path}")
try:
    df1 = extract_gti_data(file_path, pages[:1])
    df2 = extract_gti_data(file_path, pages[1:])
    df = pd.concat([df1, df2], ignore_index=True)
    df['ChangeInScore_YoY'] = df['ChangeInScore_YoY'].apply(lambda x: x.replace('GTI', '') if isinstance(x, str) else x)
    
except Exception as e:
        logging.error(f"Error loading data: {e}")
        return