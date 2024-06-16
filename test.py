import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initialize the DatabaseConnector
    db_connector = DatabaseConnector(db_creds='db_creds.yml')

    # Initialize the DataExtractor
    data_extractor = DataExtractor(db_connector)

    # Initialize the DataCleaning
    data_cleaning = DataCleaning()

    # Get the list of tables and find the user data table
    tables = db_connector.list_db_tables()

    user_data_table = None
    for table in tables:
        if 'user' in table:
            user_data_table = table
            break

    if user_data_table:
        print("Table containing user data:", user_data_table)
        
        # Get columns of the user data table
        columns = db_connector.get_table_columns(user_data_table)
        print(f"Columns in {user_data_table}: {columns}")

        # Extract the user data table
        user_data_df = data_extractor.read_rds_table(user_data_table)

        # Print the DataFrame's column names to check them
        print("Columns in the extracted DataFrame:", user_data_df.columns.tolist())
    else:
        print("No table containing user data found.")
        
if __name__ == "__main__":
    main()
