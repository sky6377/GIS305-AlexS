# WestNileVirusOutbreak GIS Analysis

This project was developed for the GIS3005 course at FRCC and simulates a public health response to a potential 
West Nile Virus outbreak using geospatial analysis.

The code uses ArcGIS Pro with ArcPy to:
- Extract address data submitted via Google Forms.
- Geocode addresses using the Nominatim OpenStreetMap API.
- Buffer key environmental layers (wetlands, lakes, mosquito sites, etc.).
- Identify safe areas for mosquito treatment by erasing avoidance zones (e.g., residential areas).
- Perform spatial joins to identify which buildings fall within treatment areas.
- Export a final map layout to PDF showing analysis results.

## Project Structure

WestNileVirusOutbreak/ │ ├── finalproject.py # Main script to run the full ETL + GIS workflow ├── etl/ │ ├── GSheetsEtl.py # Geocoding + transformation from Google Sheets │ └── SpatialEtl.py # Base ETL class ├── config/ │ └── wnvoutbreak.yaml # Configuration file for project paths and layer names ├── Output/ # Final PDF output is written here ├── WestNileOutbreak.gdb/ # File geodatabase used for spatial analysis └── README.md # This file

markdown
Copy
Edit

## Requirements

- ArcGIS Pro with access to the ArcPy package
- Python 3.x
- Internet access (for geocoding via Nominatim)
- PyCharm (or another Python IDE that supports ArcPy, if running outside ArcGIS Pro)

## How to Run

1. Update the `config/wnvoutbreak.yaml` file with your local paths and settings.
2. Make sure ArcGIS Pro is installed and licensed properly.
3. Open a Python environment where `arcpy` is available.
4. Run the script:
```bash
python finalproject.py
```
5. Follow prompts to enter:
   - Buffer distances for layers
   - Subtitle for map output
6. The final PDF map will be saved in the Output/ folder.

The final PDF map will be saved in the Output/ folder.

## Notes:
- The script sets the map’s spatial reference to **NAD 1983 StatePlane Colorado North (FIPS 0501)**.
- The geocoding step uses the Nominatim OpenStreetMap API. Ensure your User-Agent string is provided as required.


