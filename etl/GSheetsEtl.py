from SpatialEtl import SpatialEtl


class GSheetsEtl(SpatialEtl):
    # A dictionary of configuration keys and values
    config_dict = None

    def __init__(self, config_dict):
        super().__init__(config_dict)