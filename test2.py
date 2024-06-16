from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import tabula
import pandas as pd

def main():
    # Initialize the DatabaseConnector
    db_connector = DatabaseConnector(db_creds='db_creds.yml')

    # Initialize the DataExtractor
    data_extractor = DataExtractor()

    # Initialize the DataCleaning
    data_cleaning = DataCleaning()

    # PDF link
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

    # Extract data from the PDF
    extracted_data = data_extractor.retrieve_pdf_data(pdf_link)

    # Print the extracted data
    print("Extracted Data:")
    print(extracted_data)

    # Clean the card data
    cleaned_card_data = data_cleaning.clean_card_data(extracted_data)

    # Print the cleaned data
    print("Cleaned Data:")
    print(cleaned_card_data)

    # Upload the cleaned data to the dim_card_details table in the sales_data database
    db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')

    print("Cleaned card data has been uploaded to the 'dim_card_details' table in the 'sales_data' database.")

if __name__ == "__main__":
    main()
