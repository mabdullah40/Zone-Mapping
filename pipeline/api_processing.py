import googlemaps
import pandas as pd
import time
import os
from pathlib import Path


class APIGeocodingPipeline:
    def __init__(self):
        self.API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
        self.gmaps = googlemaps.Client(key="AIzaSyBSVFoiiMzR0jXbBWN7xDLYU_De1YYYmX0")
        self.input_file = Path("artifacts/regex_processing/processed_data_details.csv")
        self.output_file = Path("artifacts/api_processing/api_data_details.csv")
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def geocode_address(self, address):
        try:
            result = self.gmaps.geocode(address)
            if result:
                location = result[0]["geometry"]["location"]
                latitude = location["lat"]
                longitude = location["lng"]

                sublocality = None
                for component in result[0]["address_components"]:
                    if "sublocality_level_1" in component["types"]:
                        sublocality = component["long_name"]
                        break

                return latitude, longitude, sublocality
            else:
                print(f"No results for address: {address}")
                return None, None, None
        except Exception as e:
            print(f"Error geocoding {address}: {e}")
            return None, None, None

    def process_data(self):
        df = pd.read_csv(self.input_file)
        
        # Add new columns for latitude and longitude
        df['Latitude'] = None
        df['Longitude'] = None
        
        for index, row in df.iterrows():
            address = row['delivery_address']
            
            if pd.isna(row['L3_L4']) or row['L3_L4'] == '':
                lat, lng, sublocality = self.geocode_address(address)
                
                if sublocality:
                    df.at[index, 'L3_L4'] = sublocality
                
                df.at[index, 'Latitude'] = lat
                df.at[index, 'Longitude'] = lng
            
            # Print progress
            if index % 100 == 0:
                print(f"Processed {index} rows")
        
        return df

    def save_data(self, df):
        df.to_csv(self.output_file, index=False)
        print(f"Updated data saved to {self.output_file}")

    def main(self):
        print("Starting geocoding process...")
        df = self.process_data()
        self.save_data(df)
        print("Geocoding Completed.")



if __name__ == "__main__":
    try:
        obj = APIGeocodingPipeline()
        obj.main()
    except Exception as e:
        logging.error("An error occurred")
        logging.exception(e)
        raise e
