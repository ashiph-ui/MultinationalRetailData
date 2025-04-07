import pandas as pd
from database_utils import DatabaseConnector
import numpy as np

class DataCleaning():
    def __init__(self):
        pass
    
    def clean_user_data(self, df):
        # Replacing null values with NAN and dropping the associated rows
        df = df.replace("NULL", pd.NA)
        
        # Converting 'join_data' column to datetime datatype
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        
        # After cleaning there should be 15284 rows.
        df = df.dropna(axis=0)
        return df
    
    def clean_card_data(self, df):
        # Replacing null values with NAN and dropping the associated rows
        # 1. Change "NULL" strings to NULL/None data type
        df = df.replace("NULL", np.nan)

        # 2. Remove NULL values (rows with any NULL values)
        df = df.dropna()

        # 3. Remove duplicate card numbers (assuming the column is named 'card_number')
        df = df.drop_duplicates(subset=['card_number'])

        # 4. Remove non-numerical card numbers
        # First convert to string in case some card numbers are stored as numbers
        df['card_number'] = df['card_number'].astype(str)
        # Then keep only rows where card_number consists entirely of digits
        df = df[df['card_number'].str.isdigit()]

        # 5. Convert "date_payment_confirmed" to datetime
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

        return df

        
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