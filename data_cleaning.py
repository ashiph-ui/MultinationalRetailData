import pandas as pd
from database_utils import DatabaseConnector

class DataCleaning():
    def __init__(self):
        pass
    
    def clean_user_data(self, dta):
        # Replacing null values with NAN and dropping the associated rows
        dta = dta.replace("NULL", pd.NA)
        
        # Converting 'join_data' column to datetime datatype
        dta['join_date'] = pd.to_datetime(dta['join_date'], errors='coerce')
        
        # After cleaning there should be 15284 rows.
        dta = dta.dropna(axis=0)
        return dta
        
def main():
    from data_extraction import DataExtractor
    extractor = DataExtractor()
    db = DatabaseConnector()
    df = extractor.read_rds_table(db, 'legacy_users')
    data_cleaner = DataCleaning()
    cleaned_df = data_cleaner.clean_user_data(df)
    print(len(cleaned_df))
    

if __name__ == "__main__":
    main()        