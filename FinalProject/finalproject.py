import yaml
import arcpy
import os
import logging
import sys
from etl.GSheetsEtl import GSheetsEtl
from datetime import datetime

# --- Setup Functions ---

def setup_logging(config_dict):
    log_path = os.path.join(config_dict.get('proj_dir'), "wnv.log")
    logging.basicConfig(
        filename=log_path,
        filemode="w",
        level=logging.DEBUG
    )

def setup():
    logging.debug("Entering setup method")
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    arcpy.env.workspace = os.path.join(config_dict.get('proj_dir'), 'WestNileOutbreak.gdb')
    config_dict['gdb_path'] = arcpy.env.workspace
    arcpy.env.overwriteOutput = True
    output_folder = config_dict.get('output_folder', r"C:\Users\Owner\Documents\GIS Programming\westnileoutbreak\Output")
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
    arcpy.management.XYTableToPoint(in_table, out_feature_class, "X", "Y")
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

def etl():
    logging.info("Starting West Nile Virus Simulation")
    logging.debug("Entering etl method")
    config_dict = setup()
    process(config_dict)
    logging.debug("Exiting etl method")

# --- GIS Functions ---

def SpatialReference(aprx_path):
    # You can check if layers are in the correct spatial reference here
    map_doc = aprx.listMaps()[0]
    for layer in map_doc.listLayers():
        if layer.isFeatureLayer:
            desc = arcpy.Describe(layer)
            if desc.spatialReference.factoryCode != 3743:
                print(f"Layer {layer.name} is not in UTM Zone 13.")
    aprx.save()

def buffer(layer_name, buf_dist, config_dict):
    output_buffer_layer_path = os.path.join(config_dict.get('output_folder'), f"buf_{layer_name}.shp")
    if arcpy.Exists(layer_name):
        arcpy.analysis.Buffer(layer_name, output_buffer_layer_path, buf_dist, "FULL", "ROUND", "ALL")
        return output_buffer_layer_path
    else:
        raise FileNotFoundError(f"Input Features '{layer_name}' do not exist.")

def intersect(buffer_layer_list, config_dict):
    lyr_intersect = input("Enter the name for the intersect output layer name: ")
    lyr_intersect_path = os.path.join(config_dict.get('gdb_path'), lyr_intersect)
    arcpy.analysis.Intersect(buffer_layer_list, lyr_intersect_path, "ALL")
    return lyr_intersect_path

def erase(intersect_layer, avoid_points_buffer_layer, config_dict):
    try:
        for layer in [intersect_layer, avoid_points_buffer_layer]:
            arcpy.management.RepairGeometry(layer, "DELETE_NULL")
            if int(arcpy.management.GetCount(layer).getOutput(0)) == 0:
                raise ValueError(f"Layer {layer} is empty after repair.")
        dissolved_intersect = os.path.join(config_dict.get('gdb_path'), "Dissolved_Intersect")
        dissolved_avoid = os.path.join(config_dict.get('gdb_path'), "Dissolved_Avoid")
        arcpy.management.Dissolve(intersect_layer, dissolved_intersect)
        arcpy.management.Dissolve(avoid_points_buffer_layer, dissolved_avoid)
        erased_layer_path = os.path.join(config_dict.get('gdb_path'), "Erased_Intersect")
        arcpy.analysis.Erase(dissolved_intersect, dissolved_avoid, erased_layer_path)
        if not arcpy.Exists(erased_layer_path):
            raise RuntimeError("Failed to create erased layer.")
        return erased_layer_path
    except Exception as e:
        print(f"Error in erase operation: {str(e)}")
        raise

def spatial_join(Building_Addresses, lyr_intersect, config_dict):
    join_layer_path = os.path.join(config_dict.get('gdb_path'), "Target_Addresses")
    arcpy.analysis.SpatialJoin(
        Building_Addresses,
        lyr_intersect,
        join_layer_path,
        "KEEP_COMMON",
        match_option="INTERSECT",
    )
    return join_layer_path

def count_addresses(join_layer_path):
    return int(arcpy.management.GetCount(join_layer_path).getOutput(0))

def count_avoided_addresses(addresses_layer, erased_layer):
    avoided_layer = os.path.join(arcpy.env.workspace, "Avoided_Addresses")
    arcpy.analysis.SpatialJoin(
        addresses_layer,
        erased_layer,
        avoided_layer,
        "KEEP_COMMON",
        match_option="INTERSECT"
    )
    return int(arcpy.management.GetCount(avoided_layer).getOutput(0))

def rendering_definition_query(layer):
    layer.definitionQuery = ""

def add_to_project(layer_path, config_dict):
    aprx = arcpy.mp.ArcGISProject(os.path.join(config_dict.get('proj_dir'), "WestNileOutbreak.aprx"))
    map_doc = aprx.listMaps()[0]
    layer = map_doc.addDataFromPath(layer_path)
    if not layer:
        raise RuntimeError(f"Failed to add layer {layer_path} to the project")
    rendering_definition_query(layer)
    aprx.save()
    return layer

def rendering():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    map_doc = aprx.listMaps()[0]
    for layer in map_doc.listLayers():
        if layer.name == "Target_Addresses":
            layer.visible = True
    aprx.save()

def exportMap(config_dict):
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    layout = aprx.listLayouts()[0]
    output_pdf = os.path.join(config_dict.get('output_folder'), "WestNileOutbreak.pdf")
    layout.exportToPDF(output_pdf)

# --- Main ---

if __name__ == '__main__':
    config_dict = setup()
    arcpy.env.parallelProcessingFactor = "100%"
    etl()
    proj_path = os.path.join(config_dict.get('proj_dir'), "WestNileOutbreak.aprx")
    aprx = arcpy.mp.ArcGISProject(proj_path)
    SpatialReference(proj_path)

    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]
    for layer in buffer_layer_list:
        if arcpy.Exists(layer):
            input_distance = input(f"Enter the buffer distance for {layer} in feet: ").strip()
            input_distance_clean = ''.join(c for c in input_distance if c.isdigit())
            if not input_distance_clean:
                raise ValueError(f"Invalid buffer distance: {input_distance}")
            buffer(layer, input_distance_clean + " feet", config_dict)

    lyr_intersect_path = intersect([f"buf_{layer}" for layer in buffer_layer_list], config_dict)

    avoid_points_layer_name = config_dict.get('avoid_points_name', 'Avoid_Points')
    avoid_points_buffer_layer = buffer(avoid_points_layer_name, config_dict.get('avoid_buffer_distance', '100 feet'), config_dict)

    erased_layer_path = erase(lyr_intersect_path, avoid_points_buffer_layer, config_dict)

    avoided_count = count_avoided_addresses("Building_Addresses", erased_layer_path)
    print(f"Number of addresses outside hazard zones: {avoided_count}")

    joined_layer_path = spatial_join("Building_Addresses", lyr_intersect_path, config_dict)
    print(f"Number of addresses within the intersect layer: {count_addresses(joined_layer_path)}")

    add_to_project(joined_layer_path, config_dict)
    rendering()
    exportMap(config_dict)
    print("All operations completed successfully.")