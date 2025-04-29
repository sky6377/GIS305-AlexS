import yaml
import arcpy
import os
import logging
import time
from datetime import datetime
from etl.GSheetsEtl import GSheetsEtl

# --- Setup Functions ---

def setup_logging(config_dict):
    log_path = os.path.join(config_dict.get('proj_dir'), "wnv.log")
    logging.basicConfig(
        filename=log_path,
        filemode="w",
        level=logging.DEBUG
    )

def run_tool(tool_func, *args, **kwargs):
    tool_name = tool_func.__name__
    print(f"Starting {tool_name}...")
    start = time.time()

    result = tool_func(*args, **kwargs)

    duration = time.time() - start
    print(f"Finished {tool_name} in {duration:.2f} seconds.")
    print(arcpy.GetMessages())
    return result

def setup():
    logging.debug("Entering setup method")
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    arcpy.env.workspace = os.path.join(config_dict.get('proj_dir'), 'WestNileOutbreak.gdb')
    config_dict['gdb_path'] = arcpy.env.workspace
    arcpy.env.overwriteOutput = True
    output_folder = config_dict.get('output_folder', r"C:\\Users\\Owner\\Documents\\GIS Programming\\westnileoutbreak\\Output")
    os.makedirs(output_folder, exist_ok=True)
    config_dict['output_folder'] = output_folder
    setup_logging(config_dict)
    logging.debug("Exiting setup method")
    return config_dict

# --- ETL Functions ---

def load(config_dict):
    logging.debug("Entering load method")
    in_table = os.path.join(config_dict.get('download_dir'), 'new_addresses.csv')
    if not os.path.exists(in_table):
        raise FileNotFoundError(f"Input table '{in_table}' does not exist.")
    out_feature_class = config_dict.get('avoid_points_name', 'avoid_points')

    if arcpy.Exists(out_feature_class):
        print(f"Deleting existing {out_feature_class}...")
        run_tool(arcpy.management.Delete, out_feature_class)

    run_tool(arcpy.management.XYTableToPoint, in_table, out_feature_class, "X", "Y")

    if not arcpy.Exists(out_feature_class):
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

def etl(config_dict):
    logging.info("Starting West Nile Virus Simulation")
    logging.debug("Entering etl method")
    process(config_dict)
    logging.debug("Exiting etl method")

# --- GIS Functions ---

def spatial_reference():
    aprx = arcpy.mp.ArcGISProject(r"C:\\Users\\Owner\\Documents\\GIS Programming\\westnileoutbreak\\WestNileOutbreak\\WestNileOutbreak.aprx")
    map_doc = aprx.listMaps()[0]
    colorado_north = arcpy.SpatialReference(26953)
    map_doc.spatialReference = colorado_north
    aprx.save()
    print(f"Spatial reference set to: {colorado_north.name}")

def buffer(layer_name, buf_dist, config_dict):
    output_buffer_layer_path = os.path.join(config_dict.get('output_folder'), f"buf_{layer_name}.shp")
    if arcpy.Exists(layer_name):
        run_tool(arcpy.analysis.Buffer, layer_name, output_buffer_layer_path, buf_dist, "FULL", "ROUND", "ALL")
        return output_buffer_layer_path
    else:
        raise FileNotFoundError(f"Input Features '{layer_name}' do not exist.")

def intersect(buffer_layer_list, config_dict):
    lyr_intersect = "Intersect"
    lyr_intersect_path = os.path.join(config_dict.get('gdb_path'), lyr_intersect)
    run_tool(arcpy.analysis.Intersect, buffer_layer_list, lyr_intersect_path, "ALL")
    return lyr_intersect_path

def erase(intersect_layer, avoid_points_buffer_layer, config_dict):
    try:
        for layer in [intersect_layer, avoid_points_buffer_layer]:
            arcpy.management.RepairGeometry(layer, "DELETE_NULL")
            if int(arcpy.management.GetCount(layer)[0]) == 0:
                raise ValueError(f"Layer {layer} is empty or invalid after geometry repair.")

        dissolved_intersect = os.path.join(config_dict.get('gdb_path'), "Dissolved_Intersect")
        dissolved_avoid = os.path.join(config_dict.get('gdb_path'), "Dissolved_Avoid")
        arcpy.management.Dissolve(intersect_layer, dissolved_intersect)
        arcpy.management.Dissolve(avoid_points_buffer_layer, dissolved_avoid)

        erased_layer_path = os.path.join(config_dict.get('gdb_path'), "Final_Analysis")
        run_tool(arcpy.analysis.Erase, dissolved_intersect, dissolved_avoid, erased_layer_path)

        if not arcpy.Exists(erased_layer_path):
            raise FileNotFoundError("Failed to create erased layer.")
        return erased_layer_path
    except Exception as e:
        print(f"Error in erase: {e}")
        raise e

def spatial_join(target_layer, join_layer, config_dict):
    join_layer_path = os.path.join(config_dict.get('gdb_path'), "Target_Addresses")
    run_tool(arcpy.analysis.SpatialJoin,
             target_features=target_layer,
             join_features=join_layer,
             out_feature_class=join_layer_path,
             join_type="KEEP_COMMON",
             match_option="INTERSECT")
    return join_layer_path

def add_to_project(new_layer_path, config_dict):
    aprx = arcpy.mp.ArcGISProject(os.path.join(config_dict.get('proj_dir'), "WestNileOutbreak.aprx"))
    map_doc = aprx.listMaps()[0]
    map_doc.addDataFromPath(new_layer_path)
    aprx.save()
    print(f"Added {new_layer_path} to project.")

def export_map(config_dict):
    try:
        print("Starting export_map...")
        aprx = arcpy.mp.ArcGISProject(os.path.join(config_dict.get('proj_dir'), "WestNileOutbreak.aprx"))

        lyt_list = aprx.listLayouts("FinalProjectLayout")
        if not lyt_list:
            raise ValueError("Layout 'FinalProjectLayout' not found in project.")
        lyt = lyt_list[0]

        map_frame = lyt.listElements("MAPFRAME_ELEMENT")[0]
        map_frame.camera.scale = 49569
        map_frame.camera.X = 3079059
        map_frame.camera.Y = 1248932

        subtitle = input("Enter subtitle for map: ")
        model_run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for el in lyt.listElements("TEXT_ELEMENT"):
            if el.name == "MainTitle":
                el.text = f"West Nile Virus Outbreak Map : \"{subtitle}\""
            elif el.name == "ModelRunDate":
                el.text = f"Model Run Date: {model_run_date}"

        desired_layers = ["Target_Addresses", "Final_Analysis", "Wetlands", "OSMP_Properties", "Mosquito_Larval_Sites", "Lakes_and_Reservoirs"]

        map_doc = aprx.listMaps()[0]
        for layer in map_doc.listLayers():
            layer.visible = layer.name in desired_layers

        output_pdf = os.path.join(config_dict.get('output_folder'), "WestNileOutbreakMap.pdf")
        run_tool(lyt.exportToPDF, output_pdf)
        print(f"Map exported to {output_pdf}")

    except Exception as e:
        print(f"Error in export_map: {e}")
        raise e

# --- Main ---

if __name__ == '__main__':
    config_dict = setup()
    arcpy.env.parallelProcessingFactor = "100%"

    print("\n=== Starting ETL Process ===")
    etl(config_dict)

    print("\n=== Setting Spatial Reference ===")
    spatial_reference()

    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]

    print("\n=== Buffering Layers ===")
    for layer in buffer_layer_list:
        if arcpy.Exists(layer):
            input_distance = input(f"Enter buffer distance for {layer} in feet: ").strip()
            input_distance_clean = ''.join(c for c in input_distance if c.isdigit())
            if not input_distance_clean:
                raise ValueError(f"Invalid buffer distance: {input_distance}")
            buffer(layer, input_distance_clean + " feet", config_dict)

    print("\n=== Intersecting Buffers ===")
    intersect_layer = intersect([f"buf_{layer}" for layer in buffer_layer_list], config_dict)

    print("\n=== Buffering Avoid Points ===")
    avoid_points_buffer = buffer(config_dict.get('avoid_points_name', 'avoid_points'), config_dict.get('avoid_buffer_distance', '100 feet'), config_dict)

    print("\n=== Erasing Avoid Points ===")
    erased_layer = erase(intersect_layer, avoid_points_buffer, config_dict)

    print("\n=== Spatial Join ===")
    target_addresses = spatial_join("Building_Addresses", intersect_layer, config_dict)

    print("\n=== Adding Target Addresses to Project ===")
    add_to_project(target_addresses, config_dict)

    print("\n=== Exporting Map ===")
    export_map(config_dict)

    print("\n=== All operations completed successfully! ===")