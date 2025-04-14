import yaml
import arcpy
import os
from etl.GSheetsEtl import GSheetsEtl

def load(self):
    # Description: Creates a point feature class from input table
    # Set environment settings
    arcpy.env.workspace = f"{self.config_dict.get('proj_dir')}WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

    # Set the local variables
    in_table = f"{self.config_dict.get('download_dir')}new_addresses.csv"
    out_feature_class = self.config_dict.get('avoid_points_name', 'avoid_points')
    x_coords = "X"
    y_coords = "Y"

    # Make the XY event layer
    arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

    # Print the total rows
    print(arcpy.GetCount_management(out_feature_class))

def process(self):
    self.extract()
    self.transform()
    self.load()

def etl():
    print("etling...")
    config_dict = setup()
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.process()  # Fix: Call process()

# Global variable for the output folder
output_folder = r"C:\Users\Owner\Documents\GIS Programming\westnileoutbreak\Output"
gdb_path = r"C:\Users\Owner\Documents\GIS Programming\westnileoutbreak\WestNileOutbreak\WestNileOutbreak.gdb"

def setup():
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    # Ensure output folder exists
    output_folder = config_dict.get('output_folder', r"C:\Users\Owner\Documents\GIS Programming\westnileoutbreak\Output")
    os.makedirs(config_dict.get('output_folder'), exist_ok=True)
    config_dict['output_folder'] = output_folder  # Update config_dict with default value
    return config_dict

def buffer(layer_name, buf_dist, config_dict):
    # Buffer the incoming layer by the buffer distance
    output_buffer_layer_path = os.path.join(config_dict.get('output_folder'), f"buf_{layer_name}.shp")
    print(f"Buffering {layer_name} at {buf_dist} to generate {output_buffer_layer_path}")

    arcpy.analysis.Buffer(layer_name, output_buffer_layer_path, buf_dist, "FULL", "ROUND", "ALL")
    return output_buffer_layer_path

def intersect(buffer_layer_list):
    lyr_intersect = input("Enter the name for the intersect output layer name: ")
    lyr_intersect_path = f"{config_dict.get('gdb_path')}{lyr_intersect}"
    print(f"Intersecting {buffer_layer_list} to generate {lyr_intersect_path}")

    arcpy.analysis.Intersect(
        in_features=buffer_layer_list,
        out_feature_class=lyr_intersect_path,
        join_attributes="ALL",
        cluster_tolerance=None,
        output_type="INPUT"
    )
    return lyr_intersect_path

def erase(intersect_layer, avoid_points_buffer_layer):
    erased_layer_path = f"{config_dict.get('gdb_path')}Erased_Intersect"
    print(f"Erasing {avoid_points_buffer_layer} from {intersect_layer} to generate {erased_layer_path}")

    arcpy.analysis.Erase(
        in_features=intersect_layer,
        erase_features=avoid_points_buffer_layer,
        out_feature_class=erased_layer_path
    )
    return erased_layer_path

def spatial_join(Building_Addresses, lyr_intersect):
    join_layer_path = f"{config_dict.get('gdb_path')}Address_Join_Intersect"
    print(f"Performing spatial join with {Building_Addresses} as target and {lyr_intersect} as join feature.")

    arcpy.analysis.SpatialJoin(
        target_features=Building_Addresses,
        join_features=lyr_intersect,
        out_feature_class=join_layer_path,
        join_type="KEEP_COMMON",  # Keep only addresses within intersect area
        match_option="INTERSECT",
    )
    return join_layer_path

def count_addresses(join_layer_path):
    count_result = arcpy.management.GetCount(join_layer_path)
    count = int(count_result.getOutput(0))
    print(f"The number of addresses that fall within the intersect layer is: {count}")
    return count

def add_to_project(new_layer_path):
    proj_path = config_dict.get('proj_path')
    aprx = arcpy.mp.ArcGISProject(proj_path)
    map_doc = aprx.listMaps()[0]

    map_doc.addDataFromPath(new_layer_path)
    print(f"Added {new_layer_path} to the project.")

    aprx.save()
    print("Project saved.")

# Main script execution
if __name__ == '__main__':
    global config_dict
    config_dict = setup()
    print(config_dict)

    etl()

    # List of layers to process
    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]

    # Loop through each layer, request buffer distance, and perform the buffer operation
    for layer in buffer_layer_list:
        print(f"Processing layer: {layer}")

        # Request user input for buffer distance
        input_distance = input("Enter the buffer distance in feet: ")

        # Perform buffering
        buffer(layer, input_distance + " feet")
    print("Buffering complete.")

    # Perform intersection of buffered layers
    lyr_intersect_path = intersect([f"buf_{layer}" for layer in buffer_layer_list]) # Placeholder method call
    print("Intersect complete.")

    # Buffer the Avoid_Points layer
    avoid_points_buffer_layer = buffer(
        layer_name=config_dict.get('avoid_points_name', 'avoid_points'),
        buf_dist=config_dict.get('avoid_buffer_distance', '100 feet')  # Default buffer distance
    )
    print("Avoid points buffering complete.")

    # Perform erase analysis
    erased_layer_path = erase(lyr_intersect_path, avoid_points_buffer_layer)
    print("Erase analysis complete.")

    # Perform spatial join
    joined_layer_path = spatial_join("Building_Addresses", erased_layer_path)
    print("Spatial join complete.")

    # Count addresses within the intersect layer
    print("Counting addresses within the intersect layer...")
    address_count = count_addresses(joined_layer_path)
    print(f"Number of addresses within the intersect layer: {address_count}")

    # Add the joined feature class to the project
    add_to_project(joined_layer_path)
    print("All operations completed successfully.")