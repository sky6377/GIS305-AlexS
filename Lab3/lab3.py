import yaml
import arcpy
import os
import logging
import sys
from etl.GSheetsEtl import GSheetsEtl

def setup_logging(config_dict):
    logging.basicConfig(
        filename=f"{config_dict.get('proj_dir')}wnv.log",
        filemode="w",
        level=logging.DEBUG
    )

def load(config_dict):
    logging.debug("Entering load method")
    in_table = os.path.join(config_dict.get('download_dir'), 'new_addresses.csv')
    if not os.path.exists(in_table):
        logging.error(f"Input table '{in_table}' does not exist.")
        raise FileNotFoundError(f"Input table '{in_table}' does not exist.")

    out_feature_class = config_dict.get('avoid_points_name', 'avoid_points')
    x_coords = "X"
    y_coords = "Y"

    arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)
    logging.info("Available feature classes in the workspace: %s", arcpy.ListFeatureClasses())

    # Ensure the feature class is created successfully
    if arcpy.Exists(out_feature_class):
        logging.info(f"Feature class '{out_feature_class}' created successfully.")
    else:
        logging.error(f"Failed to create feature class '{out_feature_class}'.")
        raise FileNotFoundError(f"Failed to create feature class '{out_feature_class}'.")
    logging.debug("Exiting load method")

def process(config_dict):
    logging.debug("Entering process method")
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.extract()

    input_file = os.path.join(config_dict.get('download_dir', ''), 'raw_addresses.csv')
    output_file = os.path.join(config_dict.get('download_dir', ''), 'new_addresses.csv')

    etl_instance.transform(input_file, output_file)
    load(config_dict)
    logging.debug("Exiting process method")

def etl():
    logging.info("Starting West Nile Virus Simulation")
    logging.debug("Entering etl method")
    config_dict = setup()
    process(config_dict)
    logging.debug("Exiting etl method")

def setup():
    logging.debug("Entering setup method")
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    arcpy.env.workspace = os.path.join(config_dict.get('proj_dir'), 'WestNileOutbreak.gdb')
    arcpy.env.overwriteOutput = True
    logging.info(f"Workspace set to: {arcpy.env.workspace}")
    logging.info("Feature classes available in workspace: %s", arcpy.ListFeatureClasses())

    output_folder = config_dict.get('output_folder', r"C:\Users\Owner\Documents\GIS Programming\westnileoutbreak\Output")
    os.makedirs(output_folder, exist_ok=True)
    config_dict['output_folder'] = output_folder
    setup_logging(config_dict)
    logging.debug("Exiting setup method")
    return config_dict

def buffer(layer_name, buf_dist, config_dict):
    output_buffer_layer_path = os.path.join(config_dict.get('output_folder'), f"buf_{layer_name}.shp")
    if arcpy.Exists(layer_name):
        print(f"Buffering {layer_name} at {buf_dist} to generate {output_buffer_layer_path}")
        arcpy.analysis.Buffer(layer_name, output_buffer_layer_path, buf_dist, "FULL", "ROUND", "ALL")
        return output_buffer_layer_path
    else:
        raise FileNotFoundError(f"Input Features '{layer_name}' do not exist.")


def intersect(buffer_layer_list, config_dict):
    lyr_intersect = input("Enter the name for the intersect output layer name: ")
    lyr_intersect_path = os.path.join(config_dict.get('gdb_path'), lyr_intersect)

    print(f"Intersecting {buffer_layer_list} to generate {lyr_intersect_path}")
    arcpy.analysis.Intersect(
        in_features=buffer_layer_list,
        out_feature_class=lyr_intersect_path,
        join_attributes="ALL",
        cluster_tolerance=None,
        output_type="INPUT"
    )
    return lyr_intersect_path


def erase(intersect_layer, avoid_points_buffer_layer, config_dict):
    try:
        for layer in [intersect_layer, avoid_points_buffer_layer]:
            arcpy.management.RepairGeometry(layer, "DELETE_NULL")
            if int(arcpy.management.GetCount(layer)[0]) == 0:
                raise ValueError(f"Layer {layer} is empty or invalid after geometry repair.")

        # Optionally dissolve to avoid geometry problems
        dissolved_intersect = os.path.join(config_dict.get('gdb_path'), "Dissolved_Intersect")
        dissolved_avoid = os.path.join(config_dict.get('gdb_path'), "Dissolved_Avoid")
        arcpy.management.Dissolve(intersect_layer, dissolved_intersect)
        arcpy.management.Dissolve(avoid_points_buffer_layer, dissolved_avoid)

        erased_layer_path = os.path.join(config_dict.get('gdb_path'), "Erased_Intersect")
        print(f"Erasing {dissolved_avoid} from {dissolved_intersect} to generate {erased_layer_path}")

        arcpy.analysis.Erase(
            in_features=dissolved_intersect,
            erase_features=dissolved_avoid,
            out_feature_class=erased_layer_path
        )

        if arcpy.Exists(erased_layer_path):
            return erased_layer_path
        else:
            raise arcpy.ExecuteError("Failed to create erased layer")

    except arcpy.ExecuteError as e:
        print(f"Error in erase operation: {str(e)}")
        print(arcpy.GetMessages())
        raise
    except Exception as e:
        print(f"Unexpected error in erase operation: {str(e)}")
        raise


def spatial_join(Building_Addresses, lyr_intersect):
    join_layer_path = f"{config_dict.get('gdb_path')}/Address_Join_Intersect"
    print(f"Performing spatial join with {Building_Addresses} as target and {lyr_intersect} as join feature.")
    arcpy.analysis.SpatialJoin(
        target_features=Building_Addresses,
        join_features=lyr_intersect,
        out_feature_class=join_layer_path,
        join_type="KEEP_COMMON",
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
    raise

if __name__ == '__main__':
    config_dict = setup()
    arcpy.env.parallelProcessingFactor = "100%"  # Utilize all available cores
    logging.info("Configuration loaded: %s", config_dict)
    etl()

    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]

    for layer in buffer_layer_list:
        if arcpy.Exists(layer):
            input_distance = input("Enter the buffer distance in feet: ").strip()
            if not input_distance.replace(" ", "").replace("feet", "").isdigit():
                raise ValueError(f"Invalid buffer distance: {input_distance}")
            buffer(layer, input_distance + " feet", config_dict)
        else:
            print(f"Layer '{layer}' does not exist in the workspace.")
    print("Buffering complete.")
    print(arcpy.GetMessages())

    lyr_intersect_path = intersect([f"buf_{layer}" for layer in buffer_layer_list], config_dict)
    print("Intersect complete.")
    print(arcpy.GetMessages())

    avoid_points_layer_name = config_dict.get('avoid_points_name', 'Avoid_Points')
    avoid_points_buffer_layer = buffer(
        layer_name=avoid_points_layer_name,
        buf_dist=config_dict.get('avoid_buffer_distance', '100 feet'),
        config_dict=config_dict
    )
    print("Avoid_Points buffering complete.")
    print(arcpy.GetMessages())

    erased_layer_path = erase(lyr_intersect_path, avoid_points_buffer_layer, config_dict)
    print(f"Erased layer created at: {erased_layer_path}")

    print("All operations completed successfully.")
    print(arcpy.GetMessages())

    joined_layer_path = spatial_join("Building_Addresses", lyr_intersect_path)
    print("Spatial join complete.")
    print(arcpy.GetMessages())

    print("Counting addresses within the intersect layer...")
    address_count = count_addresses(joined_layer_path)
    if address_count == 0:
        print("No addresses found within the intersect layer.")
    else:
        print(f"Found {address_count} addresses within the intersect layer.")
    print(f"Number of addresses within the intersect layer: {address_count}")
    print(arcpy.GetMessages())

    print("Adding joined layer to the project...")
    add_to_project(joined_layer_path)

    print("Exporting map...")
    exportMap(config_dict)
    print(f"Map Exported to {config_dict.get('output_folder')}")
    print(arcpy.GetMessages())

    print("All operations completed successfully.")
    print(arcpy.GetMessages())