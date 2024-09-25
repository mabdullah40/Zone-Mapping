import pandas as pd
import mysql.connector
import logging
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataWritingPipeline:
    def __init__(self):
        self.DB_CONFIG = {
            "host": "34.126.120.50",
            "user": "masteruser1",
            "password": "lsU^$ld55UR$110",
            "database": "rider_db_orders",
        }
        self.input_file = "artifacts/warehouse_mapping/mapped_data_details.csv"
        self.batch_size = 100

    def load_data(self):
        try:
            df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(df)} rows from CSV file")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None

    def connect_to_db(self):
        try:
            connection = mysql.connector.connect(**self.DB_CONFIG)
            logger.info("Successfully connected to the database")
            return connection
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to the database: {err}")
            return None

    def update_row(self, cursor, row):
        update_query = """
        UPDATE STAGING_db_orders.OrderDetails
        SET 
            area_id_old = area_id,
            area_title_old = area_title,
            sort_addr_id_old = sort_addr_id,
            sort_addr_title_old = sort_addr_title,
            warehouse_id_old = warehouse_id,
            warehouse_title_old = warehouse_title,
            area_id = %s,
            area_title = %s,
            sort_addr_id = %s,
            sort_addr_title = %s,
            warehouse_id = %s,
            warehouse_title = %s,
            sorted_flag = 1,
            partial_match = %s
        WHERE id = %s
        """

        data = (
            row["mapped_l3_id"] if pd.notna(row["mapped_l3_id"]) else None,
            row["L3_L4"] if pd.notna(row["L3_L4"]) else None,
            row["mapped_l3_id"] if pd.notna(row["mapped_l3_id"]) else None,
            row["L3_L4"] if pd.notna(row["L3_L4"]) else None,
            (
                row["mapped_warehouse_id"]
                if pd.notna(row["mapped_warehouse_id"])
                else None
            ),
            (
                row["Mapped_Warehouse_Title"].strip()
                if pd.notna(row["Mapped_Warehouse_Title"])
                else None
            ),
            row["partial_match"] if pd.notna(row["partial_match"]) else 0,
            row["id"],
        )

        cursor.execute(update_query, data)

    def update_database(self, df):
        connection = self.connect_to_db()
        if not connection:
            return

        cursor = connection.cursor()
        total_updated = 0

        try:
            for _, row in tqdm(df.iterrows(), total=len(df), desc="Updating rows"):
                try:
                    self.update_row(cursor, row)
                    total_updated += 1

                    if total_updated % self.batch_size == 0:
                        connection.commit()
                        logger.info(f"Committed {total_updated} rows")
                except mysql.connector.Error as err:
                    logger.error(f"Error updating row {row['id']}: {err}")
                    connection.rollback()

            connection.commit()  # Commit any remaining changes
            logger.info(f"Updated {total_updated} rows in the database.")
        except Exception as e:
            logger.error(f"Error in update_database: {e}")
        finally:
            cursor.close()
            connection.close()
            logger.info("Database connection closed.")

    def main(self):
        logger.info("Starting warehouse mapping data write process...")
        df = self.load_data()

        if df is not None:
            self.update_database(df)
            logger.info("Warehouse mapping data write completed.")
        else:
            logger.error(
                "Warehouse mapping data write failed due to data loading error."
            )


if __name__ == "__main__":
    try:
        DataWritingPipeline().main()
    except Exception as e:
        logger.error("An error occurred", exc_info=True)
        raise
