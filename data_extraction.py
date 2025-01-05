import yaml
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData
from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor():
    def __init__(self ):
        pass
    
    def read_rds_table(self, table_name='legacy_users'):
        db_connector = DatabaseConnector()
        creds = db_connector.read_db_creds()
        engine = db_connector.init_db_engine(creds)
        with engine.connect() as con:
            dta = pd.read_sql(table_name, con)
        return dta
        
        
        
        
    
def main():
    obj = DataExtractor()
    obj.read_rds_table('legacy_store_details')

if __name__ == "__main__":
    main()
