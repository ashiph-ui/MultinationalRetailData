
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import tabula


class DatabaseConnector:
    def __init__(self):
        self.engine = None
        self.creds = self.read_db_creds()

    def read_db_creds(self, file_path='db_creds.yaml'):
        """Read the database credentials from a YAML file."""
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self, creds):
        """Initialise and return an SQLAlchemy database engine."""
        self.engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
            f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return self.engine

    def list_db_tables(self, engine):
        """List all tables in the database."""
        inspector = inspect(engine)
        inspector.get_table_names()
        return inspector.get_table_names()
    
    def upload_to_db(self, df, new_table_name):
        if self.engine == None:
            self.init_db_engine()
        local_db_creds = self.read_db_creds('mydb_creds.yaml')
        local_engine = self.init_db_engine(local_db_creds)

        df.to_sql(
            name=new_table_name,
            con=local_engine,
            if_exists='replace',  # Use 'append' to add data without replacing the table
            index=False
        )   
        
        
    
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