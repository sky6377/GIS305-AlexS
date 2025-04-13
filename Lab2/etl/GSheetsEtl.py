import requests
from Lab2.etl.SpatialEtl import SpatialEtl


class GSheetsEtl(SpatialEtl):
    #A dictionary of configuration keys and values
    config_dict = None
    def __init__(self, config_dict):
        super().__init__(config_dict)

    def extract(self):
        print("Extracting addresses from Google Forms spreadsheet")
        r = requests.get(self.config_dict.get('remote_url'))
        r.encoding = "utf-8"
        data = r.text
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", 'w') as output_file:
            output_file.write(data)

    def process(self):
        self.extract()
        super().transform()
        super().load()