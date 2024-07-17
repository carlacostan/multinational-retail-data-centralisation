import pandas as pd
import tabula
import requests
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initialize the DatabaseConnector for the local database
    db_connector = DatabaseConnector(db_creds='db_creds.yml')

    # Initialize the DatabaseConnector for the RDS DB 
    rds_db_connector = DatabaseConnector(db_creds='rds_db_creds.yml')

    # Initialize the DataExtractor for the local database
    data_extractor = DataExtractor(db_connector)

    # Initialize the DataExtractor for the RDS database
    rds_data_extractor = DataExtractor(rds_db_connector)

    # Initialize the DataCleaning
    data_cleaning = DataCleaning()

    # Initialize the DatabaseConnector for the local database
    db_connector = DatabaseConnector(db_creds='db_creds.yml')

    # Initialize the DataExtractor for the local database
    data_extractor = DataExtractor(db_connector)

    # Initialize the DataCleaning
    data_cleaning = DataCleaning()

   # --- Extract and clean card data from a PDF ---
    try:
        pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
        print('Extracted Data:')
        print(card_data_df)

        if not card_data_df.empty:
            cleaned_card_data = data_cleaning.clean_card_data(card_data_df)
            print("Cleaned Data:", cleaned_card_data)
            db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
            print("Cleaned card data has been uploaded to the 'dim_card_details' table in the 'sales_data' database.")
        else:
            print("No data extracted from the PDF.")
    except Exception as e:
        print(f"Error processing card data: {e}")

if __name__ == "__main__":
    main()