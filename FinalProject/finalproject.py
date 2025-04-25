import yaml
import arcpy
import os
import logging
from etl.GSheetsEtl import GSheetsEtl
from typing import List, Dict

# Constants
PARALLEL_PROCESSING_FACTOR = "100%"
AVOID_POINTS_BUFFER_DISTANCE = "100 feet"
BUFFER_LAYERS = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]

def setup_logging(config_dict):
    """Sets up logging with the project directory."""
    log_file = os.path.join(config_dict.get('proj_dir'), 'wnv.log')
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Logging initialized.")
    logging.debug("Log file created at: %s", log_file)

def validate_file_path(file_path):
    """Validates the existence of a file."""
    if not os.path.exists(file_path):
        logging.error(f"File '{file_path}' not found.")
        raise FileNotFoundError(f"File '{file_path}' does not exist.")


def load_table_to_feature_class(config_dict):
    """Loads a table and creates a feature class."""
    in_table = os.path.join(config_dict['download_dir'], 'new_addresses.csv')
    validate_file_path(in_table)

    out_feature_class = config_dict.get('avoid_points_name', 'avoid_points')
    x_coords, y_coords = "X", "Y"

    arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

    if arcpy.Exists(out_feature_class):
        logging.info(f"Feature class '{out_feature_class}' created successfully.")
    else:
        logging.error(f"Failed to create feature class '{out_feature_class}'.")
        raise RuntimeError(f"Feature class creation failed.")


def process_etl(config_dict):
    """Processes the ETL workflow."""
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.extract()

    input_file = os.path.join(config_dict['download_dir'], 'raw_addresses.csv')
    output_file = os.path.join(config_dict['download_dir'], 'new_addresses.csv')
    validate_file_path(input_file)

    etl_instance.transform(input_file, output_file)
    load_table_to_feature_class(config_dict)

def etl():
    logging.info("Starting West Nile Virus Simulation")
    logging.debug("Entering etl method")
    config_dict = setup()
    process(config_dict)
    logging.debug("Exiting etl method")


def setup_environment():
    """Initializes the project environment."""
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.safe_load(f)

    workspace = os.path.join(config_dict['proj_dir'], 'WestNileOutbreak.gdb')
    arcpy.env.workspace = workspace
    arcpy.env.overwriteOutput = True

    os.makedirs(config_dict['output_folder'], exist_ok=True)
    config_dict['output_folder'] = config_dict['output_folder']

    setup_logging(config_dict)
    logging.info(f"Workspace set to {workspace}")
    return config_dict


def buffer_layer(layer_name, buffer_distance, config_dict):
    """Creates a buffer for the given layer."""
    output_path = os.path.join(config_dict['output_folder'], f"buf_{layer_name}.shp")
    if arcpy.Exists(layer_name):
        arcpy.analysis.Buffer(layer_name, output_path, buffer_distance)
        logging.info(f"Buffered layer '{layer_name}' with distance {buffer_distance}.")
        return output_path
    else:
        logging.error(f"Layer '{layer_name}' does not exist.")
        raise RuntimeError(f"Layer '{layer_name}' not found.")


def perform_intersect(layers, config_dict):
    """Performs an intersect operation."""
    output_intersect_path = os.path.join(config_dict['output_folder'], "intersect.shp")
    arcpy.analysis.Intersect(layers, output_intersect_path)
    logging.info(f"Intersect operation completed. Output: {output_intersect_path}")
    return output_intersect_path


def erase_features(target_layer, erase_layer, config_dict):
    """Erases features from the target layer."""
    erased_output_path = os.path.join(config_dict['output_folder'], "erased_output.shp")
    arcpy.analysis.Erase(target_layer, erase_layer, erased_output_path)

    if arcpy.Exists(erased_output_path):
        logging.info(f"Erase operation successful. Output: {erased_output_path}")
    else:
        logging.error("Erase operation failed.")
        raise RuntimeError("Failed to erase features.")
    return erased_output_path


def apply_spatial_join_and_query(boulder_addresses, final_analysis, config_dict):
    """Perform spatial join and apply definition query."""
    # Define the output layer
    target_addresses = os.path.join(config_dict.get('gdb_path'), "Target_Addresses")

    # Spatial Join
    arcpy.analysis.SpatialJoin(
        target_features=boulder_addresses,
        join_features=final_analysis,
        out_feature_class=target_addresses,
        join_type="KEEP_COMMON",
        match_option="INTERSECT",
    )
    print(f"Spatial join completed. Target addresses output: {target_addresses}")

    # Apply Definition Query
    arcpy.management.MakeFeatureLayer(target_addresses, "TargetAddressesLayer")
    arcpy.management.SelectLayerByAttribute(
        "TargetAddressesLayer", "NEW_SELECTION", "Join_Count = 1"
    )
    print("Definition query applied to filter target addresses.")

    return target_addresses


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

def set_spatial_reference(config_dict):
    """Set the SpatialReference for the map document."""
    spatial_ref = arcpy.SpatialReference(102653)  # NAD 1983 StatePlane Colorado North
    arcpy.env.outputCoordinateSystem = spatial_ref
    print(f"Spatial reference set to: {spatial_ref.name}")


def apply_simple_renderer(layer_name):
    """Apply a simple renderer to the 'final_analysis' layer."""
    if arcpy.Exists(layer_name):
        arcpy.management.ApplySymbologyFromLayer(
            layer_name, symbology_layer=r"path_to_symbology.lyr"
        )
        print(f"Renderer applied to {layer_name} with 50% transparency.")
    else:
        raise FileNotFoundError(f"Layer {layer_name} does not exist.")


def apply_definition_query(target_layer, join_layer, output_layer):
    """Perform a spatial join and apply a definition query."""
    # Spatial Join
    arcpy.analysis.SpatialJoin(
        target_features=target_layer,
        join_features=join_layer,
        out_feature_class=output_layer,
        join_type="KEEP_COMMON",
        match_option="INTERSECT",
    )
    print(f"Spatial join completed. Output layer: {output_layer}")

    # Apply Definition Query
    layer_view = arcpy.management.MakeFeatureLayer(output_layer)
    arcpy.management.SelectLayerByAttribute(
        layer_view, "NEW_SELECTION", "Join_Count = 1"
    )
    print(f"Definition query applied to {output_layer}.")


def exportMap(config_dict):
    """
    Dynamically exports the map layout as a PDF with a custom subtitle.
    Args:
        config_dict (dict): Configuration dictionary containing paths and settings.
    """
    try:
        # Get the project object
        aprx = arcpy.mp.ArcGISProject(f"{config_dict.get('proj_dir')}WestNileOutbreak.aprx")

        # Get the first layout in the project
        lyt = aprx.listLayouts()[0]

        # Prompt the user for the sub-title of the output map
        subtitle = input("Enter the sub-title for the output map: ")

        # Loop through the layout elements to find the title object and update it with the user subtitle
        for el in lyt.listElements():
            print(el.name)  # Debugging to check element names
            if "Title" in el.name:  # Assumption: Title object includes 'Title' in its name
                el.text = el.text + " " + subtitle  # Appending the subtitle

        # Export the layout to a PDF file
        output_path = os.path.join(config_dict.get('output_folder'), "WestNileOutbreakMap.pdf")
        lyt.exportToPDF(output_path)
        print(f"Map exported successfully to {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise e

def process_buffer_layers(buffer_layer_list: List[str], config_dict: Dict) -> List[str]:
    """Process buffer operations for the given layers.
    
    Args:
        buffer_layer_list: List of layer names to buffer
        config_dict: Configuration dictionary
        
    Returns:
        List of processed buffer layer paths
    """
    buffered_layers = []
    for layer_name in buffer_layer_list:
        if not arcpy.Exists(layer_name):
            print(f"Layer '{layer_name}' does not exist in the workspace.")
            continue
            
        buffer_distance = get_validated_buffer_distance(layer_name)
        buffered_layer = buffer_layer(layer_name, f"{buffer_distance} feet", config_dict)
        buffered_layers.append(buffered_layer)
    
    return buffered_layers

def get_validated_buffer_distance(layer_name: str) -> str:
    """Get and validate buffer distance input from user.
    
    Args:
        layer_name: Name of the layer to get buffer distance for
        
    Returns:
        Validated buffer distance value
        
    Raises:
        ValueError: If invalid buffer distance is provided
    """
    buffer_distance = input(f"Enter the buffer distance in feet for {layer_name}: ").strip()
    if not buffer_distance.replace(" ", "").replace("feet", "").isdigit():
        raise ValueError(f"Invalid buffer distance: {buffer_distance}")
    return buffer_distance

def main():
    """Main entry point for the script."""
    config_dict = setup_environment()
    arcpy.env.parallelProcessingFactor = PARALLEL_PROCESSING_FACTOR

    # ETL Processing
    process_etl(config_dict)
    print("ETL process completed.")

    # Buffer Processing
    buffered_layers = process_buffer_layers(BUFFER_LAYERS, config_dict)
    print("Buffering complete.")
    print(arcpy.GetMessages())

    # Intersection and Erase Operations
    print("Starting Intersection and Erase Operations.")
    intersect_path = perform_intersect(buffered_layers, config_dict)
    print("Intersect operation completed.")
    
    avoid_points_buffer = buffer_layer(
        config_dict.get('avoid_points_name', 'Avoid_Points'),
        AVOID_POINTS_BUFFER_DISTANCE,
        config_dict
    )
    print("Buffering avoid points complete.")
    
    erase_features(intersect_path, avoid_points_buffer, config_dict)
    print("Erase operation completed.")
    logging.info("Script execution completed successfully.")


if __name__ == "__main__":
    main()