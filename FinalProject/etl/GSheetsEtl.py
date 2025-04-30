import csv
import arcpy
import requests
from Lab2.etl.SpatialEtl import SpatialEtl

class GSheetsEtl(SpatialEtl):
    """
    GSheetsEtl perfonrs on extract, transform, and load process using a URL to a
    Google Forms spreadsheet. The spreadsheet must contain an address and zipcode
    column.

    Parameters:
        confing_dict (dictionary): A dictionary containing a remote_url key to the
        Google spreadsheet and web geocoding service.
    """
    # A dictionary of configuration keys and values
    config_dict = None

    def __init__(self, config_dict):
        super().__init__(config_dict)

    def extract(self):
        """
        Extracting data from a Google sreadsheet and saves it to a local file.
        """
        print(GSheetsEtl.extract.__doc__)
        help(GSheetsEtl.extract)
        print("Extracting addresses from Google Forms spreadsheet")
        r = requests.get(self.config_dict.get('remote_url'))
        r.encoding = "utf-8"
        data = r.text

        # Save to download_dir/raw_addresses.csv for consistency
        extract_path = f"{self.config_dict.get('download_dir')}raw_addresses.csv"
        with open(extract_path, 'w', encoding='utf-8') as output_file:
            output_file.write(data)
        print(f"Data extracted to {extract_path}")

    def transform(self, input_file, output_file):
        """
        Transforming data using Nominatim OpenStreetMap Geocoder.
        """
        print("Transforming data using Nominatim OpenStreetMap Geocoder")
        print(GSheetsEtl.transform.__doc__)
        help(GSheetsEtl.transform)
        with open(output_file, "w", encoding='utf-8') as transformed_file:
            transformed_file.write("X,Y,Type\n")
            with open(input_file, "r", encoding='utf-8') as partial_file:
                csv_dict = csv.DictReader(partial_file, delimiter=',')
                for row in csv_dict:
                    address = row[
                                  "Street Address"] + f" {self.config_dict.get('city', 'Boulder')} {self.config_dict.get('state', 'CO')}"
                    print(f"Geocoding address: {address}")

                    geocode_url = "https://nominatim.openstreetmap.org/search"
                    params = {
                        'q': address,
                        'format': 'json',
                        'limit': 1
                    }
                    headers = {
                        'User-Agent': 'GIS305-FinalProject-Geocoder'  # Nominatim requires a User-Agent
                    }

                    try:
                        r = requests.get(geocode_url, params=params, headers=headers, timeout=10)
                        r.raise_for_status()
                        results = r.json()

                        if results:
                            lon = results[0]['lon']
                            lat = results[0]['lat']
                            transformed_file.write(f"{lon},{lat},Residential\n")
                        else:
                            print(f"No matches found for address: {address}")
                    except Exception as e:
                        print(f"Geocoding failed for address '{address}': {e}")

        print(f"Transformation complete. Data written to {output_file}")

    def load(self, input_table):
        """
        Loading transformed data into geospatial feature class.
        """
        print(GSheetsEtl.load.__doc__)
        help(GSheetsEtl.load)
        print("Loading transformed data into geospatial feature class")
        arcpy.env.workspace = self.config_dict.get('gdb_path', r"C:\default\path\to\geodatabase.gdb")
        arcpy.env.overwriteOutput = True

        out_feature_class = self.config_dict.get('avoid_points_name', 'Avoid_Points')
        x_coords = "X"
        y_coords = "Y"

        arcpy.management.XYTableToPoint(input_table, out_feature_class, x_coords, y_coords)
        print(f"Feature class '{out_feature_class}' created successfully.")

    def process(self):
        """
        Runs the full ETL process: extraction, transformation, and loading.
        """
        print(GSheetsEtl.process.__doc__)
        help(GSheetsEtl.process)
        raw_csv = f"{self.config_dict.get('download_dir')}raw_addresses.csv"
        transformed_csv = f"{self.config_dict.get('download_dir')}new_addresses.csv"

        self.extract()
        self.transform(raw_csv, transformed_csv)
        self.load(transformed_csv)
