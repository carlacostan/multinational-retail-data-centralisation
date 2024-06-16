import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests

db_connector = DatabaseConnector(db_creds='db_creds.yml')

class DataExtractor:
    def __init__(self, db_connector=None):
        """
        Initialize the DataExtractor with an instance of DatabaseConnector.

        Parameters:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
        """
        self.db_connector = db_connector
        self.api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        self.headers = {'x-api-key': self.api_key}

    def read_rds_table(self, table_name):
        """
        Read a table from the RDS database into a pandas DataFrame.

        Parameters:
        table_name (str): The name of the table to read from the database.

        Returns:
        pandas.DataFrame: A DataFrame containing the data from the table.
        """
        engine = self.db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df

    def retrieve_pdf_data(self, link):
        """
        Retrieve data from a PDF document at the specified link.

        Parameters:
        link (str): The URL of the PDF document.

        Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
        """
        try:
            # Extract all pages from the PDF
            df_list = tabula.read_pdf(link, pages='all', multiple_tables=True)

            # Concatenate all DataFrames into a single DataFrame
            extracted_data_df = pd.concat(df_list, ignore_index=True)

            # Return the extracted DataFrame
            return extracted_data_df
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            return pd.DataFrame()  # Return an empty DataFrame if extraction fails
    def list_number_of_stores(self, stores_endpoint):
        """
        Retrieve the number of stores from the API.

        Parameters:
        stores_endpoint (str): The API endpoint to retrieve the number of stores.

        Returns:
        int: The number of stores.
        """
        try:
            response = requests.get(stores_endpoint, headers=self.headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()
            return data['number_of_stores']
        except Exception as e:
            print(f"Error retrieving number of stores: {e}")
            return None