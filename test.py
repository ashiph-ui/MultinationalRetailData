from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

# Initialisation
db_connector = DatabaseConnector()
dta_cleaner = DataCleaning()
db_extracter = DataExtractor()
creds = db_connector.read_db_creds()
engine = db_connector.init_db_engine(creds)

# Get pandas df
dta = db_extracter.read_rds_table(db_connector)

# Clean df
dta_cleaned = dta_cleaner.clean_user_data(dta)
print(f"Length of data: {len(dta_cleaned)}")

# Retrive pdf
pdf_df = db_extracter.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

pdf_df_cleaned = dta_cleaner.clean_card_data(pdf_df)
print(f"Length of pdf data: {len(pdf_df_cleaned)}")

# Upload to local database
db_connector.upload_to_db(dta_cleaned, "dim_users")
db_connector.upload_to_db(pdf_df_cleaned, "dim_card_details")