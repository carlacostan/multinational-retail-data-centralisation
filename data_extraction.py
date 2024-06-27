import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3

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

            # Concatenate all DataFrames into a single DataFrame --is this necessary??
            pdf_data_df = pd.concat(df_list, ignore_index=True)

            # Return the extracted DataFrame
            return pdf_data_df
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            return pd.DataFrame()  # Return an empty DataFrame if extraction fails
        
    def list_number_of_stores(self, store_endpoint):
        """
        Retrieve the number of stores from the API.

        Parameters:
        stores_endpoint (str): The API endpoint to retrieve the number of stores.

        Returns:
        int: The number of stores.
        """
        try:
            response = requests.get(store_endpoint, headers=self.headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()
            print("API Response:", data)  # Debugging line
            return data['number_stores']
        except Exception as e:
            print(f"Error retrieving number of stores: {e}")
            return None
        
    def retrieve_stores_data(self, store_data_endpoint):
        """
        Retrieve data for all stores from the API and save them in a pandas DataFrame.

        Parameters:
        store_details_endpoint (str): The API endpoint to retrieve store details.

        Returns:
        pandas.DataFrame: A DataFrame containing details for all stores.
        """
        number_of_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
        if number_of_stores is None:
            return pd.DataFrame()  # Return an empty DataFrame if retrieval failed

        all_stores_data = []
        for store_number in range(1, number_of_stores + 1):
            url = store_data_endpoint.format(store_number=store_number) #This placeholder will be replaced by the actual store number during each iteration of the loop.

            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()  # Raise an exception for HTTP errors
                store_details = response.json()
                all_stores_data.append(store_details)
            except Exception as e:
                print(f"Error retrieving store details for store number {store_number}: {e}")

        return pd.DataFrame(all_stores_data)

    def extract_from_s3(self, s3_address):
        """
        Extract data from a CSV file stored in an S3 bucket.

        Parameters:
        s3_address (str): The S3 address of the CSV file.

        Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
        """
        s3 = boto3.client('s3')
        bucket_name, key = s3_address.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(obj['Body'])
        return df
    
    def extract_json_data(self, url):
        """
        Extract data from a JSON file stored on S3.

        Parameters:
        url (str): The URL of the JSON file.

        Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            json_data = response.json()
            df = pd.DataFrame(json_data)
            return df
        except Exception as e:
            print(f"Error extracting data from JSON: {e}")
            return pd.DataFrame()  # Return an empty DataFrame if extraction fails
        

if __name__ == "__main__":
    DataExtractor()