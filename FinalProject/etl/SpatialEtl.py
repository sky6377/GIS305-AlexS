class SpatialEtl:
    """
        Base class for ETL (Extract, Transform, Load) operations involving spatial data.

        Attributes:
            config_dict (dict): Configuration dictionary with keys for project paths and other runtime parameters.
        """

    def __init__(self, config_dict):
        """
               Initialize the SpatialEtl class with a configuration dictionary.

               Parameters:
                   config_dict (dict): Dictionary containing runtime configurations.
               """
        self.config_dict = config_dict

    def extract(self):
        """
                Stub method for data extraction to be overridden in subclasses.
                Prints the source and destination paths based on config_dict.
                """
        print(f"Extracting data from {self.config_dict['remote_url']}"
              f"to {self.config_dict['proj_dir']}")