import mysql.connector
import pandas as pd
import logging
from pathlib import Path
import hashlib


class DataIngestionPipeline:
    def __init__(self):
        # MySQL connection details
        self.DB_CONFIG = {
            "host": "34.143.155.251",  # Read DB IP
            "user": "masteruser1",
            "password": "lsU^$ld55UR$110",
            "database": "rider_db_orders",  # Replace with your actual database name
        }

        # Configure logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)
        self.output_file = Path("artifacts/data_ingestion/order_details.csv")

    def connect_to_db(self):
        try:
            connection = mysql.connector.connect(**self.DB_CONFIG)
            self.logger.info("Successfully connected to the database")
            return connection
        except mysql.connector.Error as err:
            self.logger.error(f"Error connecting to the database: {err}")
            return None

    def run_query(self, query):
        connection = self.connect_to_db()
        if connection:
            try:
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query)
                results = cursor.fetchall()
                self.logger.info("Query executed successfully")
                return results
            except mysql.connector.Error as err:
                self.logger.error(f"Error executing query: {err}")
                return None
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()
                    self.logger.info("Database connection closed")
        return None

    def get_order_details(self):
        query = """
                SELECT id,
                consignment_id,
                origin_city_id,
                origin_city_name,
                city_id,
                delivery_address,
                dest_city_name,
                warehouse_id,
                warehouse_title,
                area_id,
                area_title,
                sort_addr_id,
                sort_addr_title,
                nsa,
                area_id_old,
                area_title_old,
                sort_addr_id_old,
                sort_addr_title_old,
                warehouse_id_old,
                warehouse_title_old,
                CONCAT(area_title, ' > ', sort_addr_title) AS L3_L4,
                sorted_flag
            FROM STAGING_db_orders.OrderDetails
            WHERE DATE(booking_date) = CURRENT_DATE
            AND sorted_flag = 0   
            LIMIT 100"""

        results = self.run_query(query)
        if results:
            df = pd.DataFrame(results)
            self.logger.info("Query executed successfully.")
            return df
        else:
            self.logger.error("Query execution failed.")
            return None

    def get_data_hash(self, data):
        return hashlib.md5(pd.util.hash_pandas_object(data).values).hexdigest()

    def save_data(self, data, filename):
        filepath = Path("artifacts/data_ingestion") / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(filepath, index=False)
        self.logger.info(f"Data saved to {filepath}")

    def main(self):
        self.logger.info("Starting data ingestion process")

        # Check if the file exists
        if self.output_file.exists():
            existing_data = pd.read_csv(self.output_file)
            existing_hash = self.get_data_hash(existing_data)

            # Fetch new data
            new_data = self.get_order_details()
            if new_data is None:
                self.logger.error("Failed to fetch new data")
                return False

            new_hash = self.get_data_hash(new_data)

            if existing_hash == new_hash:
                self.logger.info("No changes in data. Stopping execution.")
                return False
            else:
                self.logger.info("Changes detected in data. Proceeding with execution.")
        else:
            self.logger.info("No existing file found. Proceeding with execution.")
            new_data = self.get_order_details()
            if new_data is None:
                self.logger.error("Failed to fetch data")
                return False

        # Save the new data
        self.save_data(new_data, "order_details.csv")
        self.logger.info("Data ingestion completed successfully")
        return True


if __name__ == "__main__":
    try:
        obj = DataIngestionPipeline()
        obj.main()
    except Exception as e:
        logging.error("An error occurred during data ingestion")
        logging.exception(e)
        raise e
