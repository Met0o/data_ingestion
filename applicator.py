from modular_dataloader import DataLoader, GTIDataLoader, WorldBankDataLoader, KnoemaDataLoader, GoogleSheetsDataLoader, SQLModulator
from utils import create_db_user_pass, api_ids
from db_config import db_credentials

create_db_user_pass(
    host=db_credentials["host"],
    database="postgres",
    user=db_credentials["user"],
    password=db_credentials["password"],
    port=db_credentials["port"],
    new_db_name=db_credentials["dbname"],
    new_user=db_credentials["new_user"],
    new_password=db_credentials["new_password"],
    superuser=True
)

common_params = {
    "host": db_credentials["host"],
    "port": db_credentials["port"],
    "dbname": db_credentials["dbname"],
    "user": db_credentials["new_user"],
    "password": db_credentials["new_password"],
}

data_loaders = {
    "earthquake_data": ("external_data", "data/earthquakes/21022023-19012022.csv", "csv", DataLoader),
    "global_terrorism_db": ("external_data", "data/globalterrorismdb_0522dist.xlsx", "xlsx", DataLoader),
    "unodc_violent_crime": ("external_data", "data/unodc/data_cts_violent_and_sexual_crime.xlsx", "xlsx", DataLoader),
    "unodc_homicide": ("external_data", "data/unodc/data_cts_intentional_homicide.xlsx", "xlsx", DataLoader),
    "unodc_corruption": ("external_data", "data/unodc/data_cts_corruption_and_economic_crime.xlsx", "xlsx", DataLoader),
    "global_terrorism_index_2022": ("external_data", "data/GTI-2022-web.pdf", [85, 86], GTIDataLoader),
    "global_terrorism_index_2023": ("external_data", "data/GTI-2023-web.pdf", [83, 84], GTIDataLoader),
    "worldbank_crime_terror": ("external_data", None, None, WorldBankDataLoader),
    "knoema_burglary": ("external_data", 'UNODCIBTHS2019', 'Location', 'Variable', 'Frequency', KnoemaDataLoader),
    "knoema_homicide": ("external_data", 'WLDHFI2022', 'Country', 'Indicator', 'Frequency', KnoemaDataLoader),
    "knoema_crime": ("external_data", 'WFGCI2019', 'Country', 'Indicator', 'Measure', 'Frequency', KnoemaDataLoader),
    "acronyms": ("internal_data", '1WVhqxCwiw9F2Z5X0nfQBztcNTT90S8kdDDIxZanMnNY', 'Acronyms!A1:ZZ', GoogleSheetsDataLoader),
    "project_assets": ("internal_data", '1WVhqxCwiw9F2Z5X0nfQBztcNTT90S8kdDDIxZanMnNY', 'Project Assets!A1:ZZ', GoogleSheetsDataLoader)
}

sql_files = [
    ("Creating the country table", "data/sql/country_data.sql"),
    ("Creating the mapping table", "data/sql/country_name_mapping.sql"),
    ("Creating full country mapping inside a view", "data/sql/proper_country.sql"),
    ("Updating the tables with country_code column - GTI", "data/sql/update_gti_tables.sql"),
    ("Updating the tables with country_code column - Knoema", "data/sql/update_knoema_tables.sql"),
    ("Updating the tables with country_code column - World Bank", "data/sql/update_worldbank_tables.sql"),
    ("Setting up primary key constraints", "data/sql/pk_update.sql"),
    ("Creating Country Metrics - Knoema Middle East", "data/sql/knoema_middle_east_data_full.sql"),
    ("Creating Country Metrics - Knoema Middle East Latest Record", "data/sql/knoema_middle_east_data_latest_record.sql"),
    ("Creating Country Metrics - Terrorism Middle East", "data/sql/terrorism_middle_east.sql"),
    ("Creating Country Metrics - UNODC Middle East", "data/sql/unodc_middle_east_data.sql"),
    ("Creating Country Metrics - World Bank Middle East", "data/sql/worldbank_middle_east.sql"),
]

for table_name, values in data_loaders.items():
    schema_name, *args, loader_class = values
    loader_params = {**common_params, 'schema_name': schema_name, 'table_name': table_name}
    loader = loader_class(**loader_params)

    if loader_class == DataLoader:
        data_path, file_type = args
        loader.load_data(data_path=data_path, file_type=file_type)

    elif loader_class == GTIDataLoader:
        data_path, pages = args
        loader.load_data(file_path=data_path, pages=pages)

    elif loader_class == WorldBankDataLoader:
        loader.load_data(api_ids)

    elif loader_class == KnoemaDataLoader:
        dataset_id, *id_vars = args
        loader.load_data(dataset_id, *id_vars)

    elif loader_class == GoogleSheetsDataLoader:
        spreadsheet_id, range_name = args
        loader.load_data(spreadsheet_id=spreadsheet_id, range_name=range_name)
        
for description, file_path in sql_files:
    sql_modulator = SQLModulator(sql_file_path=file_path, **common_params)
    sql_modulator.execute_sql_from_file()