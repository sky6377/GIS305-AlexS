class SpatialEtl:

    def __init__(self, config_dict):
        self.config_dict = config_dict

    def extract(self):
        print(f"Extracting data from {self.config_dict['remote_url']}"
              f"to {self.config_dict['proj_dir']}")