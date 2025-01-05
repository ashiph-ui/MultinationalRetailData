
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    def read_db_creds(self, file_path='db_creds.yaml'):
        """Read the database credentials from a YAML file."""
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self, creds):
        """Initialise and return an SQLAlchemy database engine."""
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
            f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self, engine):
        """List all tables in the database."""
        inspector = inspect(engine)
        inspector.get_table_names()
        return inspector.get_table_names()
    
    def upload_to_db(self):
        creds = self.read_db_creds('mydb_creds.yaml')
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
            f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        
        from sqlalchemy import text
        with engine.connect() as connection:
            inspector = inspect(engine)
            print(inspector.get_table_names())
        
        
        

    
def main():
    db_connector = DatabaseConnector()
    creds = db_connector.read_db_creds()
    engine = db_connector.init_db_engine(creds)

    # List tables
    tables = db_connector.list_db_tables(engine)
    print("Available tables:", tables)
    db_connector.upload_to_db()

if __name__ == "__main__":
    main()