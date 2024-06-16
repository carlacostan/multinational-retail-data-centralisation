from datetime import datetime
import pandas as pd

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