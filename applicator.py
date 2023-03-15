from modular_dataloader import DataLoader, GTIDataLoader, WorldBankDataLoader, KnoemaDataLoader, GoogleSheetsDataLoader
from utils import api_ids

host="100.81.10.110"
port=31035 
dbname="AIR"
user="writedata"
password="writedataAdmin12%$"

# Loading data from the Global Terrorism DB

loader_terrorismdb = DataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                        schema_name="external_data",
                        table_name = "global_terrorism_db")
loader_terrorismdb.load_data(data_path="data/globalterrorismdb_0522dist.xlsx", file_type='xlsx')

# Loading data from the United Nations Drugs and Crime DB

loader_unodc_violent_crime = DataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                       schema_name="external_data",
                       table_name = "unodc_violent_crime")
loader_unodc_violent_crime.load_data(data_path="data/unodc/data_cts_violent_and_sexual_crime.xlsx", file_type='xlsx')

loader_unodc_homicide = DataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                       schema_name="external_data",
                       table_name = "unodc_homicide")
loader_unodc_homicide.load_data(data_path="data/unodc/data_cts_intentional_homicide.xlsx", file_type='xlsx')

loader_unodc_corruption = DataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                       schema_name="external_data",
                       table_name = "unodc_corruption")
loader_unodc_corruption.load_data(data_path="data/unodc/data_cts_corruption_and_economic_crime.xlsx", file_type='xlsx')

# Loading data that has been scraped from the GTI 2022 report.

loader_gti = GTIDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                                schema_name="external_data",
                                table_name="global_terrorism_index")
loader_gti.load_data(data_path="data/GTI-2022-web.pdf")

# Loading data from the WorldBank API

loader_worldbank = WorldBankDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                                schema_name="external_data",
                                table_name="worldbank_crime_terror")
loader_worldbank.load_data(api_ids)

# Loading data from Knoema.com

loader_knoema_burglary = KnoemaDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                               schema_name="external_data",
                               table_name="knoema_burglary")
loader_knoema_burglary.load_data('UNODCIBTHS2019', 'Location', 'Variable', 'Frequency')

loader_knoema_homicide = KnoemaDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                               schema_name="external_data",
                               table_name="knoema_homicide")
loader_knoema_homicide.load_data('WLDHFI2022', 'Country', 'Indicator', 'Frequency')

loader_knoema_crime = KnoemaDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                               schema_name="external_data",
                               table_name="knoema_crime")
loader_knoema_crime.load_data('WFGCI2019', 'Country', 'Indicator', 'Measure', 'Frequency')

# Loading data from the Google Sheets Master DB file:

loader_googlesheets_acronyms = GoogleSheetsDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                                schema_name="internal_data",
                                table_name="acronyms")
loader_googlesheets_acronyms.load_data(spreadsheet_id='1WVhqxCwiw9F2Z5X0nfQBztcNTT90S8kdDDIxZanMnNY', range_name='Acronyms!A1:ZZ')

loader_googlesheets_assets = GoogleSheetsDataLoader(host=host,port=port,dbname=dbname,user=user,password=password, 
                                schema_name="internal_data",
                                table_name="project_assets")
loader_googlesheets_assets.load_data(spreadsheet_id='1WVhqxCwiw9F2Z5X0nfQBztcNTT90S8kdDDIxZanMnNY', range_name='Project Assets!A1:ZZ')