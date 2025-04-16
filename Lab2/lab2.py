import yaml
import arcpy
import os
from etl.GSheetsEtl import GSheetsEtl

def etl():
    print("ETLing...")
    config_dict = setup()
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.process()

def buffer(layer_name, buf_dist, config_dict):
    output_buffer_layer_path = os.path.join(config_dict.get('output_folder'), f"buf_{layer_name}.shp")
    if arcpy.Exists(layer_name):
        print(f"Buffering {layer_name} at {buf_dist} to generate {output_buffer_layer_path}")
        arcpy.analysis.Buffer(layer_name, output_buffer_layer_path, buf_dist, "FULL", "ROUND", "ALL")
        return output_buffer_layer_path
    else:
        raise FileNotFoundError(f"Input Features '{layer_name}' do not exist.")

def erase(intersect_layer, avoid_points_buffer_layer, config_dict):
    erased_layer_path = os.path.join(config_dict.get('gdb_path'), "Erased_Intersect")
    print(f"Erasing {avoid_points_buffer_layer} from {intersect_layer} to generate {erased_layer_path}")
    arcpy.analysis.Erase(
        in_features=intersect_layer,
        erase_features=avoid_points_buffer_layer,
        out_feature_class=erased_layer_path
    )
    return erased_layer_path

if __name__ == '__main__':
    global config_dict
    config_dict = setup()
    print(config_dict)

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

    lyr_intersect_path = intersect([f"buf_{layer}" for layer in buffer_layer_list], config_dict)
    print("Intersect complete.")

    avoid_points_layer_name = config_dict.get('avoid_points_name', 'Avoid_Points')
    avoid_points_buffer_layer = buffer(
        layer_name=avoid_points_layer_name,
        buf_dist=config_dict.get('avoid_buffer_distance', '100 feet'),
        config_dict=config_dict
    )
    print("Avoid_Points buffering complete.")

    erased_layer_path = erase(lyr_intersect_path, avoid_points_buffer_layer, config_dict)
    print(f"Erased layer created at: {erased_layer_path}")

    print("All operations completed successfully.")