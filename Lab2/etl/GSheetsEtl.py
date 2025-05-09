import csv
import arcpy
import requests
from Lab2.etl.SpatialEtl import SpatialEtl

class GSheetsEtl(SpatialEtl):
    # A dictionary of configuration keys and values
    config_dict = None

    def __init__(self, config_dict):
        super().__init__(config_dict)

    def extract(self):
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
        print("Transforming data by adding City, State")
        with open(output_file, "w", encoding='utf-8') as transformed_file:
            transformed_file.write("X,Y,Type\n")
            with open(input_file, "r", encoding='utf-8') as partial_file:
                csv_dict = csv.DictReader(partial_file, delimiter=',')
                for row in csv_dict:
                    address = row["Street Address"] + f" {self.config_dict.get('city', 'Boulder')} {self.config_dict.get('state', 'CO')}"
                    print(f"Geocoding address: {address}")
                    geocode_url = (
                        f"{self.config_dict.get('geocode_base_url', 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress')}"
                        f"?address={address}&benchmark={self.config_dict.get('geocode_benchmark', '2020')}&format=json"
                    )
                    r = requests.get(geocode_url)
                    resp_dict = r.json()

                    if resp_dict['result']['addressMatches']:
                        x = resp_dict['result']['addressMatches'][0]['coordinates']['x']
                        y = resp_dict['result']['addressMatches'][0]['coordinates']['y']
                        transformed_file.write(f"{x},{y},Residential\n")
                    else:
                        print(f"No matches found for address: {address}")

        print(f"Transformation complete. Data written to {output_file}")

    def load(self, input_table):
        print("Loading transformed data into geospatial feature class")
        arcpy.env.workspace = self.config_dict.get('gdb_path', r"C:\default\path\to\geodatabase.gdb")
        arcpy.env.overwriteOutput = True

        out_feature_class = self.config_dict.get('avoid_points_name', 'Avoid_Points')
        x_coords = "X"
        y_coords = "Y"

        arcpy.management.XYTableToPoint(input_table, out_feature_class, x_coords, y_coords)
        print(f"Feature class '{out_feature_class}' created successfully.")

    def process(self):
        raw_csv = f"{self.config_dict.get('download_dir')}raw_addresses.csv"
        transformed_csv = f"{self.config_dict.get('download_dir')}new_addresses.csv"

        self.extract()
        self.transform(raw_csv, transformed_csv)
        self.load(transformed_csv)
