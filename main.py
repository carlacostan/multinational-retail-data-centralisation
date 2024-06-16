import pandas as pd
import tabula
import requests
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initialize the DatabaseConnector
    db_connector = DatabaseConnector(db_creds='db_creds.yml')

    # Initialize the database engine
    engine = db_connector.init_db_engine()

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
        # Extract the user data table
        user_data_df = data_extractor.read_rds_table(user_data_table)

        # Clean the user data
        cleaned_user_data_df = data_cleaning.clean_user_data(user_data_df)

        # Upload the cleaned data to the dim_users table
        db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')

        print(cleaned_user_data_df)
    else:
        print('No table containing user data found.')

     # PDF link
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

    # Extract data from the PDF
    extracted_pdf_data = data_extractor.retrieve_pdf_data(pdf_link)

    # Print the extracted data
    print('Extracted Data:')
    print(extracted_pdf_data)

    # Clean the card data
    cleaned_card_data = data_cleaning.clean_card_data(extracted_pdf_data)

    # Print the cleaned data
    print(f'Cleaned Data:{cleaned_card_data}')

    # Upload the cleaned data to the dim_card_details table in the sales_data database
    db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')

    print("Cleaned card data has been uploaded to the 'dim_card_details' table in the 'sales_data' database.")

    #Extract data from the API
    
    stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_of_stores = data_extractor.list_number_of_stores(stores_endpoint)

    #Print number of stores
    print(f"Number of stores:{number_of_stores}")

if __name__ == "__main__":
    main()

    