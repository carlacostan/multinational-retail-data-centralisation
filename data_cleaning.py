from datetime import datetime
import pandas as pd
import re

class DataCleaning:
    def clean_user_data(self, user_data_df):
        """
        Perform cleaning of the user data.

        Parameters:
        user_data_df (pandas.DataFrame): DataFrame containing the user data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame containing the user data.
        """
        # Handle NULL values: For this example, we will drop rows with any NULL values.
        user_data_df = user_data_df.dropna()

        # Fix errors with dates and convert to datetime if necessary
        user_data_df['date_of_birth'] = pd.to_datetime(user_data_df['date_of_birth'], errors='coerce')
        user_data_df['join_date'] = pd.to_datetime(user_data_df['join_date'], errors='coerce')

        # Remove rows with NULL dates after conversion
        user_data_df = user_data_df.dropna(subset=['date_of_birth', 'join_date'])

        # Correct data types: Example for ensuring UUID is a string
        user_data_df['user_uuid'] = user_data_df['user_uuid'].astype(str)

        # Example checks for logical consistency
        # Ensure join_date is after date_of_birth
        user_data_df = user_data_df[user_data_df['join_date'] > user_data_df['date_of_birth']]

        # Assuming further columns like phone_number should be valid:
        user_data_df = user_data_df[user_data_df['phone_number'].apply(lambda x: x.isdigit())]

        # Remove rows with duplicates in 'user_uuid'
        user_data_df = user_data_df.drop_duplicates(subset=['user_uuid'])

        # Additional cleaning steps as needed...

        return user_data_df
    
    def clean_card_data(self, card_data_df):
        """
        Clean the card data DataFrame by removing erroneous values, NULL values, and formatting errors.

        Parameters:
        card_data_df (pandas.DataFrame): DataFrame containing the card data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame containing the card data.
        """
        # Remove rows with NULL values
        card_data_df = card_data_df.dropna()

        # Convert date columns to datetime format
        card_data_df['expiry_date'] = pd.to_datetime(card_data_df['expiry_date'], format='%m/%y', errors='coerce')
        card_data_df['date_payment_confirmed'] = pd.to_datetime(card_data_df['date_payment_confirmed'], errors='coerce')

        # Drop rows where date conversion failed (resulting in NaT)
        card_data_df = card_data_df.dropna(subset=['expiry_date', 'date_payment_confirmed'])

        # Ensure card_number is treated as a string
        card_data_df['card_number'] = card_data_df['card_number'].astype(str)

        # Additional cleaning steps can be added as needed...

        return card_data_df
    
    def clean_store_data(self, stores_data_df):
        """
        Clean the store data DataFrame by removing erroneous values, NULL values, and formatting errors.

        Parameters:
        all_stores_data (pandas.DataFrame): DataFrame containing the store data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame containing the store data.
        """
        # Remove rows with any NULL values
        stores_data_df = stores_data_df.dropna()

        # Check if 'date_opened' column exists and convert it to datetime format
        if 'date_opened' in stores_data_df.columns:
            stores_data_df['date_opened'] = pd.to_datetime(stores_data_df['date_opened'], errors='coerce')

        # Drop rows where 'date_opened' conversion resulted in NaT (Not a Time, indicating parsing errors)
        stores_data_df = stores_data_df.dropna(subset=['date_opened'])

        # Ensure 'store_number' is treated as a string
        stores_data_df['store_number'] = stores_data_df['store_number'].astype(str)

        # Remove rows with duplicate 'store_number' values
        stores_data_df = stores_data_df.drop_duplicates(subset=['store_number'])

        return stores_data_df
    
    def convert_product_weights(self, products_df):
        """
        Convert product weights to a uniform decimal value representing kilograms.

        Parameters:
        products_df (pandas.DataFrame): DataFrame containing the product data.

        Returns:
        pandas.DataFrame: DataFrame with the weight column standardized to kg.
        """
        def convert_weight(weight):
            if isinstance(weight, float):
                return weight  # Return the weight if it's already a float
            weight = str(weight).lower()

            # Handle cases like "12 x 100g"
            if ' x ' in weight:
                parts = weight.split(' x ')
                try:
                    return float(parts[0]) * float(parts[1].replace('g', '').strip()) / 1000
                except ValueError:
                    return None  # Handle conversion error

            try:
                if 'kg' in weight:
                    return float(weight.replace('kg', '').strip())
                elif 'g' in weight:
                    return float(weight.replace('g', '').strip()) / 1000
                elif 'ml' in weight:
                    return float(weight.replace('ml', '').strip()) / 1000
                elif 'l' in weight:
                    return float(weight.replace('l', '').strip())
                else:
                    # Extract numeric values and try to convert
                    match = re.search(r"(\d+(\.\d+)?)", weight)
                    if match:
                        return float(match.group(1))
                    return None  # Handle unexpected formats
            except ValueError:
                return None  # If conversion fails, return None

        products_df['weight'] = products_df['weight'].apply(convert_weight)
        return products_df

    def clean_products_data(self, products_df):
        """
        Clean the products data by removing any additional erroneous values.

        Parameters:
        products_df (pandas.DataFrame): DataFrame containing the product data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        products_df = products_df.dropna(subset=['weight'])
        products_df = products_df.drop_duplicates(subset=['uuid'])
        return products_df
    
    def clean_orders_data(self, orders_df):
        """
        Clean the orders data by removing unnecessary columns.
        """
        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'])
        return orders_df
    
    def clean_date_data(self, date_df):
        """
        Clean the date data.

        Parameters:
        date_df (pandas.DataFrame): DataFrame containing the date data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame containing the date data.
        """
        # Specify the format to ensure consistent parsing
        date_format = '%H:%M:%S'
        date_df['timestamp'] = pd.to_datetime(date_df['timestamp'], format=date_format, errors='coerce')
        date_df = date_df.dropna(subset=['timestamp'])
        return date_df
 
if __name__ == "__main__":
    DataCleaning()