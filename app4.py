import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from streamlit_folium import st_folium
from folium.plugins import Draw
from arcgis.gis import GIS
from arcgis.mapping import WebMap
from arcgis.features import FeatureSet
from pyproj import Transformer
import numpy as np
import time

# Initialize session state for geometries if not already done
if 'geojson_list' not in st.session_state:
    st.session_state.geojson_list = []
if 'metadata_list' not in st.session_state:
    st.session_state.metadata_list = []
if 'map_initialized' not in st.session_state:
    st.session_state.map_initialized = False
if 'table_columns' not in st.session_state:
    st.session_state.table_columns = {}

# Database connection function
def get_connection():
    try:
        conn = psycopg2.connect(
            host=st.secrets["db_host"],
            database=st.secrets["db_name"],
            user=st.secrets["db_user"],
            password=st.secrets["db_password"],
            port=st.secrets["db_port"]
        )
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

# Query all tables with a "SHAPE" column
def get_tables_with_shape_column():
    conn = get_connection()
    if conn is None:
        return []
    try:
        query = """
        SELECT table_name 
        FROM information_schema.columns 
        WHERE column_name = 'SHAPE' AND table_schema = 'public';
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df['table_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching table names: {e}")
        return []

# Get column names for a specific table
def get_table_columns(table_name):
    conn = get_connection()
    if conn is None:
        return []
    try:
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public';
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df['column_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching columns for table {table_name}: {e}")
        return []

# Query geometries within a polygon for a specific table
def query_geometries_within_polygon_for_table(table_name, polygon_geojson):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        query = f"""
        SELECT *, "SHAPE"::text as geometry, srid, drawing_info::text as drawing_info
        FROM public.{table_name}
        WHERE ST_Intersects(
            ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON("SHAPE"::json), srid), 4326),
            ST_SetSRID(
                ST_GeomFromGeoJSON('{polygon_geojson}'),
                4326
            )
        );
        """

        df = pd.read_sql(query, conn)
        conn.close()

        # Ensure no duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]

        return df
    except Exception as e:
        st.error(f"Query error in table {table_name}: {e}")
        return pd.DataFrame()

# Query geometries within a polygon for all relevant tables
def query_geometries_within_polygon(polygon_geojson):
    tables = get_tables_with_shape_column()
    all_data = []
    
    progress_bar = st.progress(0)
    total_tables = len(tables)
    
    for idx, table in enumerate(tables):
        df = query_geometries_within_polygon_for_table(table, polygon_geojson)
        if not df.empty:
            df['table_name'] = table
            all_data.append(df)
            st.session_state.table_columns[table] = get_table_columns(table)
        progress_bar.progress((idx + 1) / total_tables)

    # Filter out empty DataFrames before concatenation
    all_data = [df for df in all_data if not df.empty]

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# Function to add geometries to map with coordinate transformation
def add_geometries_to_map(geojson_list, metadata_list, map_object):
    for geojson, metadata in zip(geojson_list, metadata_list):
        if 'srid' not in metadata:
            continue

        srid = metadata.pop('srid')
        table_name = metadata.pop('table_name')
        drawing_info_str = metadata.pop('drawing_info', '{}')
        drawing_info_1 = json.dumps(drawing_info_str)
        drawing_info = json.loads(drawing_info_1)
        geometry = json.loads(geojson)

        # Define the source and destination coordinate systems
        src_crs = pyproj.CRS(f"EPSG:{srid}")
        dst_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        # Transform the geometry to the geographic coordinate system
        shapely_geom = shape(geometry)
        transformed_geom = transform(transformer.transform, shapely_geom)

        # Remove the 'geometry' and 'SHAPE' fields from metadata for the popup
        metadata.pop('geometry', None)
        metadata.pop('SHAPE', None)

        # Filter metadata to include only columns from the respective table
        table_columns = st.session_state.table_columns.get(table_name, [])
        filtered_metadata = {key: value for key, value in metadata.items() if key in table_columns and pd.notna(value) and value != ''}

        # Extract style information from drawing_info
        style = {}
        if 'renderer' in drawing_info:
            renderer = drawing_info['renderer']
            if 'symbol' in renderer:
                symbol = renderer['symbol']
                if 'color' in symbol:
                    style['color'] = f"rgba({symbol['color'][0]},{symbol['color'][1]},{symbol['color'][2]},{symbol['color'][3] / 255})"
                if 'outline' in symbol and 'color' in symbol['outline']:
                    style['outline_color'] = f"rgba({symbol['outline']['color'][0]},{symbol['outline']['color'][1]},{symbol['outline']['color'][2]},{symbol['outline']['color'][3] / 255})"
                    
        # Create a popup with metadata (other columns)
        metadata_html = f"<b>Table: {table_name}</b><br>" + "<br>".join([f"<b>{key}:</b> {value}" for key, value in filtered_metadata.items()])
        popup = folium.Popup(metadata_html, max_width=300)

        if transformed_geom.geom_type == 'Point':
            folium.Marker(location=[transformed_geom.y, transformed_geom.x], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in transformed_geom.coords], popup=popup, color=style.get('color')).add_to(map_object)
        elif transformed_geom.geom_type == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords], popup=popup, color=style.get('color'), fill_color=style.get('outline_color')).add_to(map_object)
        elif transformed_geom.geom_type == 'MultiLineString':
            for line in transformed_geom.geoms:
                folium.PolyLine(locations=[(coord[1], coord[0]) for coord in line.coords], popup=popup, color=style.get('color')).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {transformed_geom.geom_type}")

def df_to_geojson(df):
    """Convert DataFrame with geometry and metadata to GeoJSON."""
    def convert_value(value):
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if pd.isna(value):
            return None
        return value

    features = []
    for _, row in df.iterrows():
        properties = {key: convert_value(value) for key, value in row.drop("geometry").to_dict().items()}
        feature = {
            "type": "Feature",
            "geometry": json.loads(row["geometry"]),
            "properties": properties,
        }
        features.append(feature)
    return json.dumps({"type": "FeatureCollection", "features": features})

# Function to transform coordinates in a GeoJSON
def transform_geojson(geojson_data, from_srid, to_srid):
    transformer = Transformer.from_crs(from_srid, to_srid, always_xy=True)
    for feature in geojson_data['features']:
        coords = feature['geometry']['coordinates']
        if not coords:  # Skip empty coordinate arrays
            continue
        if feature['geometry']['type'] == "Point":
            feature['geometry']['coordinates'] = transformer.transform(*coords)
        elif feature['geometry']['type'] == "MultiPoint":
            feature['geometry']['coordinates'] = [transformer.transform(*coord) for coord in coords if isinstance(coord, (list, tuple)) and len(coord) == 2]
        elif feature['geometry']['type'] == "LineString":
            feature['geometry']['coordinates'] = [transformer.transform(*coord) for coord in coords if isinstance(coord, (list, tuple)) and len(coord) == 2]
        elif feature['geometry']['type'] == "MultiLineString":
            transformed_lines = []
            for line in coords:
                transformed_lines.append([transformer.transform(*coord) for coord in line if isinstance(coord, (list, tuple)) and len(coord) == 2])
            feature['geometry']['coordinates'] = transformed_lines
        elif feature['geometry']['type'] == "Polygon":
            feature['geometry']['coordinates'] = [[transformer.transform(*coord) for coord in ring if isinstance(coord, (list, tuple)) and len(coord) == 2] for ring in coords]
        elif feature['geometry']['type'] == "MultiPolygon":
            feature['geometry']['coordinates'] = [[[transformer.transform(*coord) for coord in ring if isinstance(coord, (list, tuple)) and len(coord) == 2] for ring in polygon] for polygon in coords]
        elif feature['geometry']['type'] == "GeometryCollection":
            feature['geometry']['geometries'] = [transform_geojson({'type': 'Feature', 'geometry': geom, 'properties': {}}, from_srid, to_srid)['geometry'] for geom in feature['geometry']['geometries']]
    return geojson_data


# Initialize Streamlit app
st.title('Streamlit Map Application')

# Create a Folium map centered on Los Angeles if not already done
def initialize_map():
    m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)
    draw = Draw(
        export=True,
        filename='data.geojson',
        position='topleft',
        draw_options={'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False},
        edit_options={'edit': False}
    )
    draw.add_to(m)
    return m

if not st.session_state.map_initialized:
    st.session_state.map = initialize_map()
    st.session_state.map_initialized = True

# Handle the drawn polygon
st_data = st_folium(st.session_state.map, width=700, height=500, key="initial_map")

if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
    polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
    
    if st.button('Query Database'):
        try:
            df = query_geometries_within_polygon(polygon_geojson)
            #st.write(df.head())
            if not df.empty:
                st.session_state.geojson_list = df['geometry'].tolist()
                st.session_state.metadata_list = df.to_dict(orient='records')
                
                # Clear the existing map and reinitialize it
                m = initialize_map()
                add_geometries_to_map(st.session_state.geojson_list, st.session_state.metadata_list, m)
                st.session_state.map = m
                
                # Convert DataFrame to GeoJSON
                geojson_data = json.loads(df_to_geojson(df))
                
                # Transform coordinates in GeoJSON
                transformed_geojson = transform_geojson(geojson_data, "EPSG:3857", "EPSG:4326")
                
                                # Provide download link for the results
                st.download_button(
                    label="Download Geometries as GeoJSON",
                    data=json.dumps(transformed_geojson),
                    file_name="geometries.geojson",
                    mime="application/json"
                )
                
                # Authenticate with ArcGIS Online
                gis = GIS("https://www.arcgis.com", st.secrets["arcgis_username"], st.secrets["arcgis_password"])
                
                # Create a new web map
                webmap = WebMap()
                
                # Function to create layers based on drawing info styles
                def create_layers_by_styles(features):
                    style_dict = {}
                    for feature in features:
                        drawing_info = feature.get('properties', {}).get('drawing_info')
                        if drawing_info:
                            style_key = json.dumps(drawing_info)
                            if style_key not in style_dict:
                                style_dict[style_key] = {
                                    "features": [],
                                    "drawing_info": drawing_info
                                }
                            style_dict[style_key]["features"].append(feature)
                    return style_dict
                
                # Extract features and create layers based on drawing info
                features = transformed_geojson['features']
                styled_layers = create_layers_by_styles(features)
                #st.write(features)
                #st.write(styled_layers)
                

                
                # Iterate over each styled layer and add it to the web map
                for i, (style_key, styled_layer) in enumerate(styled_layers.items(), 1):
                    layer_name = styled_layer["features"][0]["properties"]["table_name"]
                    st.write(i)
                    # Create a GeoJSON dictionary for this layer
                   # geojson_layer = {
                     #   "type": "FeatureCollection",
                    #    "features": styled_layer["features"]
                   # }

                    geojson_layer = {
                        "type": "FeatureCollection",
                        "features": transformed_geojson['features']
                    }
                                   
                    # Convert the GeoJSON to a FeatureSet
                    fs = FeatureSet.from_geojson(geojson_layer)
                    
                    
                    
                    # Extract the renderer from the drawing info
                    #renderer = styled_layer.get("drawing_info", {}).get("renderer", {})
                    
                    # Add the FeatureSet as a layer to the web map with a title and renderer
                    webmap.add_layer(fs, {
                        "title": layer_name,
                        
                    })
                   # "renderer": renderer
                #st.info(fs.features)

                
                # Save the web map as a new item in ArcGIS Online
                coordinates = st_data['last_active_drawing']['geometry']['coordinates']
                print(f"Coordinates: {coordinates}")
                
                xmin = coordinates[0][0][0]
                ymin = coordinates[0][0][1]
                xmax = coordinates[0][2][0]
                ymax = coordinates[0][2][1]
                print(f"xmin: {xmin}, ymin: {ymin}, xmax: {xmax}, ymax: {ymax}")
                
                webmap_properties = {
                    "title": "Web Map with Styled GeoJSON Layers",
                    "snippet": "A web map that includes layers with different drawing styles",
                    "tags": ["GeoJSON", "Web Map"],
                    "extent": {
                        "spatialReference": {"wkid": 4326},
                        "xmin": xmin,
                        "ymin": ymin,
                        "xmax": xmax,
                        "ymax": ymax
                    }
                }
                webmap_item = webmap.save(item_properties=webmap_properties)
                
                # Make the web map public
                webmap_item.share(everyone=True)
                
                # Print the link to the web map
                webmap_url = f"https://www.arcgis.com/home/webmap/viewer.html?webmap={webmap_item.id}"
                st.info(fs.features)
                st.info(f"Web map saved and made public. [View the web map]({webmap_url})")
                st.success(f"Web map saved with ID: {webmap_item.id}")
                

            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

# Display the map using Streamlit-Folium
#st_folium(st.session_state.map, width=700, height=500, key="map")
