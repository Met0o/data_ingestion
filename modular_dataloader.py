import os.path
import logging
import pandas as pd
import requests
import psycopg2
import knoema

from PyPDF2 import PdfReader
from psycopg2.extras import execute_values
from tqdm import tqdm

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

def sheets_to_dataframe(spreadsheet_id, range_name):
    
    """Retrieve data from a Google Sheets file and create a Pandas DataFrame.

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

def extract_gti_data(pages):
    
    """
    Extracts data from Global Terrorism Index 2022 PDF report.

    Args:
        pages: A list of page numbers to extract data from.

    Returns:
        A pandas dataframe containing the extracted data with the following columns:
            * GTI_Rank: The rank of the country based on the Global Terrorism Index score.
            * Country: The name of the country.
            * GTI_Score_2021: The Global Terrorism Index score of the country in 2021.
            * ChangeInScore_2020_2021: The change in the Global Terrorism Index score of the country from 2020 to 2021.
    """
    
    reader = PdfReader("data/GTI-2022-web.pdf")
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
            
    return pd.DataFrame(data, columns=['GTI_Rank', 'Country', 'GTI_Score_2021', 'ChangeInScore_2020_2021'])

class BaseDataLoader:
    
    """
    Base class for data loaders. Inheriting classes should implement the `load_data` function.

    Args:
        host (str): Host name or IP address of the database server.
        port (int): Port number of the database server.
        dbname (str): Name of the database to connect to.
        user (str): Username to use for authentication.
        password (str): Password to use for authentication.
        schema_name (str): Name of the schema to use for the database table.
        table_name (str): Name of the database table to use for storing the data.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema_name = schema_name
        self.table_name = table_name

    def load_data(self, data_path):
        """
        Placeholder function for data loading. This function should be implemented in inheriting classes.
        """
        pass

    def post_load(self, conn, cur, df):
        pass

class GoogleSheetsDataLoader(BaseDataLoader):
    
    """
    This class connects to a Google Sheets file, normally stored on a Google Drive using the Sheets API (requires google project).
    It retrieves and processes data for a given Sheet ID, and loads it into a PostgreSQL database.

    Args:
        host (str): Host name or IP address of the database server.
        port (int): Port number of the database server.
        dbname (str): Name of the database to connect to.
        user (str): Username to use for authentication.
        password (str): Password to use for authentication.
        schema_name (str): Name of the schema to use for the database table.
        table_name (str): Name of the database table to use for storing the data.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        super().__init__(host, port, dbname, user, password, schema_name, table_name)

    def load_data(self, spreadsheet_id, range_name):
        
        """
        Loads data from the Google Sheets into a PostgreSQL database.

        Args:
            spreadsheet_id (List[str]): An ID of the Sheets file to use for retrieving data (requires permissions to the file).
            range [str]: Spreadsheet name and range selected. E.g.: Acronyms!A1:ZZ, where 'Acronyms' is the sheet name and 'A1:ZZ' extracts the full content of the sheet.
        """
        logging.info(f"Loading data from Google Sheets: {spreadsheet_id}")
        try:
            
            if 'acronyms' in self.table_name:
                
                df = sheets_to_dataframe(spreadsheet_id, range_name)
                df = df.iloc[3:].iloc[:, 1:].rename(columns={'Column 2': 'Term', 'Column 3': 'Overview', 'Column 4': 'Description', 'Column 5': 'Notes'})
                
                create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (Term text, Overview text, Description text, Notes text);"
            
            elif 'project_assets' in self.table_name:
                
                df = sheets_to_dataframe(spreadsheet_id, range_name)
                df = df.iloc[2:].iloc[:, 1:].rename(columns={'Column 2': 'Master_Asset_List', 'Column 3': 'scope', 'Column 4': 'hotel', 'Column 5': 'notes'})
                
                create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (Master_Asset_List text, Scope text, Hotel text, notes text);"
            
            else:
                raise ValueError("Invalid table or column names.")
            
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return

        logging.info("Connecting to database...")
        try:
            with psycopg2.connect(host=self.host, port=self.port, database=self.dbname, user=self.user, password=self.password) as conn:
                with conn.cursor() as cur:
                    try:
                        
                        logging.info("Creating schema...")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
                        conn.commit()
                        
                        logging.info("Dropping the table if it exists...")
                        cur.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}")
                        conn.commit()
                        
                        logging.info("Creating table...")
                        cur.execute(create_query)
                        conn.commit()

                        logging.info("Loading data into db...")
                        with tqdm(total=len(df)) as pbar:
                            execute_values(cur, f"INSERT INTO {self.schema_name}.{self.table_name} VALUES %s", df.values.tolist(), page_size=50000)
                            pbar.update(len(df))
                            
                    except Exception as e:
                        logging.error(f"Error creating table: {e}")
                        conn.rollback()
                        return
                    
                    logging.info("Committing changes to database...")
                    conn.commit()

                    logging.info("Testing if data has been loaded successfully...")
                    cur.execute(f"SELECT * FROM {self.schema_name}.{self.table_name} LIMIT 5")
                    result = cur.fetchone()

                    if result is not None:
                        logging.info("Data load successful.")
                    else:
                        logging.error("Data load failed!")
        
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return

class WorldBankDataLoader(BaseDataLoader):
    
    """
    Data loader for World Bank data. This class connects to the World Bank API, retrieves data for a given set of API IDs,
    and loads it into a PostgreSQL database using 'execute_values' which is the most optimal method for large files.

    Args:
        host (str): Host name or IP address of the database server.
        port (int): Port number of the database server.
        dbname (str): Name of the database to connect to.
        user (str): Username to use for authentication.
        password (str): Password to use for authentication.
        schema_name (str): Name of the schema to use for the database table.
        table_name (str): Name of the database table to use for storing the data.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        super().__init__(host, port, dbname, user, password, schema_name, table_name)

    def load_data(self, api_ids):
        
        """
        Loads data from the World Bank API into a PostgreSQL database.

        Args:
            api_ids (List[str]): A list of API IDs to use for retrieving data.
        """
        
        logging.info("Loading data from World Bank API...")
        try:
            data_list = []
            for id in api_ids:
                api_endpoint = fr'https://api.worldbank.org/v2/country/all/indicator/{id}?format=json&per_page=18000'
                response = requests.get(api_endpoint)
                data = response.json()

                for obs in data[1]:
                    data_dict = {}
                    data_dict["metric"] = obs["indicator"]["value"]
                    data_dict["country"] = obs["country"]["value"]
                    data_dict["year"] = obs["date"]
                    data_dict["value"] = obs["value"]
                    data_list.append(data_dict)

            df = pd.DataFrame(data_list)
            df['year'] = pd.to_datetime(df['year'], format='%Y')

            create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (metric text, country text, year date, value numeric);"

        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return

        logging.info("Connecting to database...")
        try:
            with psycopg2.connect(host=self.host, port=self.port, database=self.dbname, user=self.user, password=self.password) as conn:
                with conn.cursor() as cur:
                    try:
                        
                        logging.info("Creating schema...")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
                        conn.commit()
                        
                        logging.info("Dropping the table if it exists...")
                        cur.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}")
                        conn.commit()
                        
                        logging.info("Creating table...")
                        cur.execute(create_query)
                        conn.commit()

                        logging.info("Loading data into db...")
                        with tqdm(total=len(df)) as pbar:
                            execute_values(cur, f"INSERT INTO {self.schema_name}.{self.table_name} VALUES %s", df.values.tolist(), page_size=50000)
                            pbar.update(len(df))
                            
                    except Exception as e:
                        logging.error(f"Error creating table: {e}")
                        conn.rollback()
                        return
                    
                    logging.info("Committing changes to database...")
                    conn.commit()

                    logging.info("Testing if data has been loaded successfully...")
                    cur.execute(f"SELECT * FROM {self.schema_name}.{self.table_name} LIMIT 5")
                    result = cur.fetchone()

                    if result is not None:
                        logging.info("Data load successful.")
                    else:
                        logging.error("Data load failed!")
        
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return


class KnoemaDataLoader(BaseDataLoader):
    
    """
    Data loader for Knoema data. This class connects to the Knoema API, retrieves data for a given API call, and loads
    it into a PostgreSQL database using 'execute_values' which is the most optimal method for large files.

    Args:
        host (str): Host name or IP address of the database server.
        port (int): Port number of the database server.
        dbname (str): Name of the database to connect to.
        user (str): Username to use for authentication.
        password (str): Password to use for authentication.
        schema_name (str): Name of the schema to use for the database table.
        table_name (str): Name of the database table to use for storing the data.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        super().__init__(host, port, dbname, user, password, schema_name, table_name)

    def load_data(self, api_call, *id_vars):
        
        """
        Loads data from the Knoema API into a PostgreSQL database.

        Args:
            api_call (str): The API call to use for retrieving data.
            id_vars (str): The variable(s) to use as identifier columns in the resulting DataFrame.

        Raises:
            ValueError: If the data frame columns are not valid.
        """
        
        logging.info(f"Loading data from Knoema API: {api_call}")
        try:
            
            apicfg = knoema.ApiConfig()
            apicfg.host = 'knoema.com'
            apicfg.app_id = 'NKo0wLI'
            apicfg.app_secret = 'rS7t12PuWsokQ'
            
            df = knoema.get(api_call)
            df = df.transpose().reset_index()
            df.columns = df.columns.get_level_values(0)
            df = pd.melt(df, id_vars=id_vars, var_name='Date', value_name='Value')
            df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
            
            if 'knoema_burglary' in self.table_name:
                create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (Location text, Variable text, Frequency text, Date date, Value numeric);"
            elif 'knoema_homicide' in self.table_name:
                create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (Country text, Indicator text, Frequency text, Date date, Value numeric);"
            elif 'knoema_crime' in self.table_name:
                create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (Country text, Indicator text, Measure text, Frequency text, Date date, Value numeric);"
            else:
                raise ValueError("Invalid column names in data frame.")
            
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return

        logging.info("Connecting to database...")
        try:
            with psycopg2.connect(host=self.host, port=self.port, database=self.dbname, user=self.user, password=self.password) as conn:
                with conn.cursor() as cur:
                    try:
                        logging.info("Creating schema...")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
                        conn.commit()
                        
                        logging.info(f"Dropping the table if it exists...")
                        cur.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}")
                        conn.commit()
                        
                        logging.info(f"Creating table...")
                        cur.execute(create_query)
                        conn.commit()

                        logging.info(f"Loading data into db...")
                        with tqdm(total=len(df)) as pbar:
                            execute_values(cur, f"INSERT INTO {self.schema_name}.{self.table_name} ({', '.join(df.columns)}) VALUES %s", df.values.tolist(), page_size=50000)
                            pbar.update(len(df))
                            
                    except Exception as e:
                        logging.error(f"Error creating table: {e}")
                        conn.rollback()
                        return
                    
                    logging.info("Committing changes to database...")
                    conn.commit()

                    logging.info("Testing if data has been loaded successfully...")
                    cur.execute(f"SELECT * FROM {self.schema_name}.{self.table_name} LIMIT 5")
                    result = cur.fetchone()

                    if result is not None:
                        logging.info("Data load successful.")
                    else:
                        logging.error("Data load failed!")
        
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return

   
class TerrorismDataLoader(BaseDataLoader):
    
    """
    A class for transforming terrorism data from the Global Terrorism Database (GTD).

    Attributes:
        host (str): The hostname of the PostgreSQL database.
        port (str): The port number of the PostgreSQL database.
        dbname (str): The name of the PostgreSQL database.
        user (str): The username for connecting to the PostgreSQL database.
        password (str): The password for connecting to the PostgreSQL database.
        schema_name (str): The name of the schema in the PostgreSQL database where the data will be loaded.
        table_name (str): The name of the table in the PostgreSQL database where the data will be loaded.

    Methods:
        post_load(conn, cur, df): A method that performs additional processing steps after loading the terrorism data into the PostgreSQL database.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        super().__init__(host, port, dbname, user, password, schema_name, table_name)
        self.schema_name = schema_name
        self.table_name = table_name
        
    def post_load(self, conn, cur, df):
        try:
            logging.info("Updating the table...")
            cur.execute(f"ALTER TABLE {self.schema_name}.{self.table_name} ADD COLUMN eventdate date")
            cur.execute("UPDATE external_data.global_terrorism_db SET eventdate = TO_DATE(iyear::text || '-' || imonth || '-' || iday, 'YYYY-MM-DD')")
            cur.execute(f"ALTER TABLE {self.schema_name}.{self.table_name} ALTER COLUMN eventdate TYPE date USING eventdate::date")
            conn.commit()
            
            logging.info("Dropping unwanted columns...")
            cur.execute(f"ALTER TABLE {self.schema_name}.{self.table_name} DROP COLUMN iyear, DROP COLUMN imonth, DROP COLUMN iday, DROP COLUMN extended, DROP COLUMN alternative, DROP COLUMN compclaim;")
            cur.execute(f"ALTER TABLE {self.schema_name}.{self.table_name} DROP COLUMN attacktype2, DROP COLUMN attacktype3, DROP COLUMN targtype2, DROP COLUMN targsubtype2, DROP COLUMN natlty2;")
            cur.execute(f"ALTER TABLE {self.schema_name}.{self.table_name} DROP COLUMN targtype3, DROP COLUMN targsubtype3, DROP COLUMN natlty3, DROP COLUMN guncertain2, DROP COLUMN guncertain3, DROP COLUMN claimmode3;")
            cur.execute(f"ALTER TABLE {self.schema_name}.{self.table_name} DROP COLUMN nperps, DROP COLUMN nperpcap, DROP COLUMN claimed, DROP COLUMN claimmode, DROP COLUMN claim2, DROP COLUMN claim3, DROP COLUMN claimmode2;")
            conn.commit()
            
            logging.info("Testing if data has been loaded successfully...")
            cur.execute(f"SELECT * FROM {self.schema_name}.{self.table_name} LIMIT 5")
            result = cur.fetchone()
            conn.commit()

            if result is not None:
                logging.info("Data load successful.")
            else:
                logging.error("Data load failed!")
                
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return


class GTIDataLoader(BaseDataLoader):
    
    """
    A class for loading Global Terrorism Index (GTI) data from a PDF file into a PostgreSQL database,
    using 'execute_values' which is the most optimal method for large files.

    Attributes:
        host (str): The hostname of the PostgreSQL database.
        port (str): The port number of the PostgreSQL database.
        dbname (str): The name of the PostgreSQL database.
        user (str): The username for connecting to the PostgreSQL database.
        password (str): The password for connecting to the PostgreSQL database.
        schema_name (str): The name of the schema in the PostgreSQL database where the data will be loaded.
        table_name (str): The name of the table in the PostgreSQL database where the data will be loaded.

    Methods:
        load_data: Loads GTI data from a PDF file into a PostgreSQL database.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        super().__init__(host, port, dbname, user, password, schema_name, table_name)

    def load_data(self, data_path):
        
        """
        Loads GTI data from a PDF file into a PostgreSQL database.

        Args:
            data_path (str): The path to the PDF file containing the GTI data.

        Returns:
            None
        """
        
        logging.info(f"Loading data from PDF file: {data_path}")
        try:
            df1 = extract_gti_data([85])
            df2 = extract_gti_data([86])
            df = pd.concat([df1, df2], ignore_index=True)
            df['ChangeInScore_2020_2021'] = df['ChangeInScore_2020_2021'].apply(lambda x: x.replace('GTI', '') if isinstance(x, str) else x)
            
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return

        logging.info("Connecting to database...")
        try:
            with psycopg2.connect(host=self.host, port=self.port, database=self.dbname, user=self.user, password=self.password) as conn:
                with conn.cursor() as cur:
                    try:
                        logging.info("Creating schema...")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
                        conn.commit()
                        
                        logging.info("Dropping the table if it exists...")
                        cur.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}")
                        conn.commit()
                        
                        logging.info("Creating table...")
                        create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} (GTI_Rank integer, Country text, GTI_Score_2021 double precision, ChangeInScore_2020_2021 double precision)"
                        cur.execute(create_query)
                        conn.commit()

                        logging.info("Loading data into db...")
                        with tqdm(total=len(df)) as pbar:
                            execute_values(cur, f"INSERT INTO {self.schema_name}.{self.table_name} ({','.join(df.columns)}) VALUES %s", df.values.tolist(), page_size=50000)
                            pbar.update(len(df))
                            
                    except Exception as e:
                        logging.error(f"Error creating table: {e}")
                        conn.rollback()
                        return
                    
                    logging.info("Committing changes to database...")
                    conn.commit()

                    logging.info("Testing if data has been loaded successfully...")
                    cur.execute(f"SELECT * FROM {self.schema_name}.{self.table_name} LIMIT 5")
                    result = cur.fetchone()

                    if result is not None:
                        logging.info("Data load successful.")
                    else:
                        logging.error("Data load failed!")
        
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return

class DataLoader:
    
    """
    A class for loading data from Excel or CSV files into a PostgreSQL database using 'execute_values' which is the most optimal method for large files.

    Attributes:
        host (str): The host name or IP address of the PostgreSQL server.
        port (str): The port number of the PostgreSQL server.
        dbname (str): The name of the database to connect to.
        user (str): The username for the PostgreSQL account.
        password (str): The password for the PostgreSQL account.
        schema_name (str): The name of the schema where the data will be stored.
        table_name (str): The name of the table where the data will be stored.
        type_map (dict): A mapping of Pandas data types to PostgreSQL data types.

    Methods:
        load_data(data_path, file_type='xlsx'): Loads data from an Excel or CSV file into a PostgreSQL table.
    """
    
    def __init__(self, host, port, dbname, user, password, schema_name, table_name):
        
        """
        Initializes a DataLoader instance with the specified parameters.

        Args:
            host (str): The host name or IP address of the PostgreSQL server.
            port (str): The port number of the PostgreSQL server.
            dbname (str): The name of the database to connect to.
            user (str): The username for the PostgreSQL account.
            password (str): The password for the PostgreSQL account.
            schema_name (str): The name of the schema where the data will be stored.
            table_name (str): The name of the table where the data will be stored.
        """
        
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema_name = schema_name
        self.table_name = table_name
        
        self.type_map = {
            'int64': 'bigint',
            'float64': 'double precision',
            'object': 'text',
            'bool': 'boolean',
            'datetime64[ns]': 'text'
        }
        
    def load_data(self, data_path, file_type='xlsx'):
        
        """
        Loads data from an Excel or CSV file into a PostgreSQL table.

        Args:
            data_path (str): The path to the file containing the data.
            file_type (str, optional): The type of file to load. Can be 'xlsx' or 'csv'. Defaults to 'xlsx'.
        """
        
        logging.info(f"Loading {file_type} data from file...")
        try:
            with open(data_path, 'rb') as f:
                with tqdm(total=os.path.getsize(data_path), unit='B', unit_scale=True) as pbar:
                    if file_type == 'xlsx':
                        df = pd.read_excel(f, engine='openpyxl', verbose=False)
                    elif file_type == 'csv':
                        df = pd.read_csv(f)
                    else:
                        raise ValueError(f"Unsupported file type: {file_type}")
                    pbar.update(os.path.getsize(data_path))
                    
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return
        
        if self.table_name == "global_terrorism_db":
            try:
                logging.info("Dropping unwanted columns & filling the blanks...")
                df = df.drop(df.columns[[0, 4, 6]], axis=1)
                df = df.where(df.notna(), None)
            except Exception as e:
                logging.error(f"Error dropping columns: {e}")
                conn.rollback()
                return

        if self.table_name.startswith('unodc'):
            try:
                logging.info("Converting 'year' column to date format...")
                df['Year'] = pd.to_datetime(df['Year'], format='%Y')
            except Exception as e:
                logging.error(f"Error converting column type: {e}")
                conn.rollback()
                return
        
        logging.info("Connecting to database...")
        try:
            with psycopg2.connect(host=self.host, port=self.port, database=self.dbname, user=self.user, password=self.password) as conn:
                with conn.cursor() as cur:
                    try:
                        logging.info("Creating schema...")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
                        conn.commit()
                        
                        logging.info("Dropping the table if exists...")
                        cur.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}")
                        conn.commit()
                                
                        logging.info("Creating table...")
                        create_query = f"CREATE TABLE {self.schema_name}.{self.table_name} ({', '.join([f'{col} {self.type_map[str(df[col].dtype)]}' for col in df.columns])})"
                        cur.execute(create_query)
                        conn.commit()
                        
                        logging.info("Loading table into db...")
                        with tqdm(total=len(df)) as pbar:
                            execute_values(cur, f"INSERT INTO {self.schema_name}.{self.table_name} ({','.join(df.columns)}) VALUES %s", df.values.tolist(), page_size=50000)
                            pbar.update(len(df))
                            
                    except Exception as e:
                        logging.error(f"Error creating table: {e}")
                        conn.rollback()
                        return
                    
                    if self.table_name == "global_terrorism_db":
                        try:
                            TerrorismDataLoader(self.host, self.port, self.dbname, self.user, self.password, self.schema_name, self.table_name).post_load(conn, cur, df)
                        except Exception as e:
                            logging.error(f"Error updating table: {e}")
                            conn.rollback()
                            return

                    logging.info("Committing changes to database...")
                    conn.commit()

                    logging.info("Testing if data has been loaded successfully...")
                    cur.execute(f"SELECT * FROM {self.schema_name}.{self.table_name} LIMIT 5")
                    result = cur.fetchone()

                    if result is not None:
                        logging.info("Data load successful.")
                    else:
                        logging.error("Data load failed!")
        
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return