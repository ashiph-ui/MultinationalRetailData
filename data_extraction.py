import yaml
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData
from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor():
    def __init__(self ):
        pass
    
    def read_rds_table(self, db_instance, table_name='legacy_users'):
        creds = db_instance.read_db_creds()
        engine = db_instance.init_db_engine(creds)
        with engine.connect() as con:
            dta = pd.read_sql(table_name, con)
        return dta
    
    def retrieve_pdf_data(self, url):
        dfs = tabula.read_pdf(url, stream=True)
        return dfs           
    
def main():
    obj = DataExtractor()
    db = DatabaseConnector()
    dta = obj.read_rds_table(db, 'legacy_store_details')
    print(dta)

if __name__ == "__main__":
    main()
