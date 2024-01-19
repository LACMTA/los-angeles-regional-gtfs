import sys, argparse

# from calendar import calendar
import os
print(os.getcwd())
print(os.listdir())
# exit()

import pandas as pd
import json
import datetime
import geopandas as gpd
import geoalchemy2
from io import StringIO
import zipfile
import timeit
import shutil

from pathlib import Path
from sqlalchemy import create_engine,MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from shapely.geometry import Point, LineString

debug = False
local = False

list_of_gtfs_static_files = ["routes", "trips", "stops", "calendar", "shapes","stop_times"]

# Argument parser for database connections
parser = argparse.ArgumentParser(description='Process database URI.')
parser.add_argument('--db_uri', metavar='db_uri', type=str, nargs='+',
                    help='The postgresql database URI for updating the GTFS Static data to.', required=True)

parser.add_argument('--db_schema', metavar='db_schema', type=str, nargs='+',help='Target postgresql database schema for updating.', required=True)


args = parser.parse_args()
DB_URI = args.db_uri[0]
TARGET_SCHEMA = args.db_schema[0]
engine = create_engine(DB_URI, echo=False, pool_size=20, max_overflow=0, pool_timeout=300, pool_recycle=3600, pool_pre_ping=True)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

session = Session()
Base = declarative_base(metadata=MetaData(schema=TARGET_SCHEMA))

df_to_combine = []


def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


def combine_dataframes(temp_df_bus,temp_df_rail):
    return pd.concat([temp_df_bus, temp_df_rail])

def create_list_of_trips(trips,stop_times):
    print('Creating list of trips')
    trips_list_df = stop_times.groupby('trip_id')['stop_sequence'].max().sort_values(ascending=False).reset_index()
    trips_list_df.set_index(['trip_id','stop_sequence'], inplace=True)
    stop_times.set_index(['trip_id','stop_sequence'], inplace=True)
    trips_list_df = trips_list_df.join(stop_times[['stop_id','route_code']])
    return trips_list_df

def update_dataframe_to_db(combined_temp_df,target_table_name,engine,target_schema):
    print('Updating dataframe to db')
    combined_temp_df.to_sql(target_table_name,engine,index=False,if_exists="replace",schema=target_schema)


def process_zip_files_for_agency_id(agency_id):
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    target_zip_files = None
    if agency_id is None:
        print('No agency_id provided.')
        sys.exit(0)
    if agency_id == 'lacmta':
        target_zip_files = get_latest_modified_zip_file(root_dir / 'lacmta/', 'metro_api', agency_id)
        replace_and_archive_file(target_zip_files, root_dir / 'lacmta/current-base/gtfs_bus.zip', root_dir / 'lacmta/current-base/archive')
    if agency_id == 'lacmta-rail':
        target_zip_files = get_latest_modified_zip_file(root_dir / 'lacmta-rail/', 'metro_api', agency_id)
    extract_zip_file_to_temp_directory(agency_id)


def get_latest_modified_zip_file(path, target_schema, agency_id):
    
    print(path)
    print(target_schema)
    if target_schema == 'metro_api_future':
        target_path = os.path.join(path, "future")
    elif target_schema == 'metro_api':
        if agency_id == 'lacmta-rail':
            target_path = os.path.join(path, "current")
        else:
            target_path = os.path.join(path, "current-base")
    else:
        print('Invalid target_schema provided.')
        sys.exit(1)
    if not os.path.exists(target_path):
        print('No such directory: ' + target_path)
        sys.exit(1)
    try:
        zip_files = [os.path.join(target_path, f) for f in os.listdir(target_path) if f.endswith('.zip') and f != 'gtfs_bus.zip']
        if zip_files:
            return max(zip_files, key=os.path.getmtime)
        else:
            print("No zip files to process.")
            sys.exit(0)  # Exit the script with a success status
    except Exception as e:
        print('Error getting latest modified zip file: ' + str(e))
        sys.exit(1)


def replace_and_archive_file(source_file, target_file, archive_dir):
    # If the target file exists and is named 'gtfs_bus.zip', move it to the archive directory with a timestamp

    shutil.copy2(source_file, archive_dir)
    # Copy the source file to the target directory and rename it to 'gtfs_bus.zip'
    # Assuming target_file is the full path to the file
    new_target_file = os.path.join(os.path.dirname(target_file), 'gtfs_bus.zip')

    # Delete the new_target_file if it exists
    if os.path.exists(new_target_file):
        os.remove(new_target_file)

    os.rename(source_file, new_target_file)

def extract_zip_file_to_temp_directory(agency_id):
    zip_file = None
    if agency_id == 'lacmta':
        if TARGET_SCHEMA == 'metro_api_future':
            zip_file = '../lacmta/future/gtfs_bus.zip'
        elif TARGET_SCHEMA == 'metro_api':
            zip_file = '../lacmta/current-base/gtfs_bus.zip'
    elif agency_id == 'lacmta-rail':
        zip_file = '../lacmta-rail/current/gtfs_rail.zip'
    try:
        print('Extracting zip file to temp directory: ' + zip_file)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('../temp/'+agency_id)
    except Exception as e:
        print('Error extracting zip file to temp directory: ' + str(e))
        sys.exit(1)

#### END FILE EXTRACTION ####

#### START GTFS STATIC PROCESSING ####
def update_gtfs_static_files():
    global stop_times_df
    global trips_df
    global calendar_dates_df
    global calendar_df
    global stops_df
    for file in list_of_gtfs_static_files:
        print("******************")
        print("Starting with " + file)
        process_start = timeit.default_timer()
        bus_file_path = ""
        rail_file_path = ""

        bus_file_path = "../temp/lacmta/" + file + '.txt'
        rail_file_path = "../temp/lacmta-rail/" + file + '.txt'
        temp_df_bus = pd.read_csv(bus_file_path)
        temp_df_bus['agency_id'] = 'LACMTA'
        temp_df_rail = pd.read_csv(rail_file_path)
        temp_df_rail['agency_id'] = 'LACMTA_Rail'
        if file == "stops":
            stops_df = update_stops_seperately(temp_df_bus,temp_df_rail,file)
        elif file == "shapes":
            shapes_combined_gdf = create_gdf_for_shapes(temp_df_bus,temp_df_rail)
            if debug == False:
                shapes_combined_gdf.to_postgis(file,engine,index=False,if_exists="replace",schema=TARGET_SCHEMA)
        elif file == "stop_times":            
            temp_df_rail['trip_id'] = temp_df_rail['trip_id'].astype(str)
            temp_df_bus['trip_id'] = temp_df_bus['trip_id'].astype(str)
            cols = ['pickup_type','drop_off_type']
            combined_temp_df = combine_dataframes(temp_df_bus,temp_df_rail)
            combined_temp_df['rider_usage_code_before_coding'] = combined_temp_df[cols].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
            combined_temp_df['rider_usage_code'] = combined_temp_df['rider_usage_code_before_coding'].apply(lambda x: 1 if x == '00' else 2 if x == '10' else 3 if x == '01' else 0 if x == '11' else -1)
            if 'bay_num' not in combined_temp_df.columns:
                combined_temp_df['bay_num'] = -1
            combined_temp_df.drop(columns=['rider_usage_code_before_coding'])
            stop_times_df = combined_temp_df
            if debug == False:
                update_dataframe_to_db(combined_temp_df,file,engine,TARGET_SCHEMA)

        else:
            combined_temp_df = combine_dataframes(temp_df_bus,temp_df_rail)

            if file == "trips":
                trips_df = combined_temp_df
            if file == "calendar_dates":
                calendar_dates_df = combined_temp_df
            if file == "calendar":
                calendar_df = combined_temp_df
            if debug == False:
                update_dataframe_to_db(combined_temp_df,file,engine,TARGET_SCHEMA)
                # combined_temp_df.to_sql(file,engine,index=False,if_exists="replace",schema=TARGET_SCHEMA)
        process_end = timeit.default_timer()
        
        with open('../logs.txt', 'a+') as f:
            human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_time = process_end - process_start
            total_time_rounded = round(total_time,2)
            print(human_readable_date+" | " + file + " | " + str(total_time_rounded) + " seconds.", file=f)
            print("******************")
    print("Processing trip list")
    process_start = timeit.default_timer()
    trips_list_df = create_list_of_trips(trips_df,stop_times_df)
    summarized_trips_df = trips_df[["route_id","trip_id","direction_id","service_id","agency_id"]]
    summarized_trips_df['day_type'] = summarized_trips_df['service_id'].map(get_day_type_from_service_id)
    trips_list_df = trips_list_df.merge(summarized_trips_df, on='trip_id').drop_duplicates(subset=['route_id','day_type','direction_id'])

    trips_list_df.apply(lambda row: get_stop_times_for_trip_id(row,stop_times_df), axis=1)
    stop_times_by_route_df = pd.concat(df_to_combine)
    stop_times_by_route_df['departure_times'] = stop_times_by_route_df.apply(lambda row: get_stop_times_from_stop_id(row),axis=1)
    stop_times_by_route_df['route_code'].fillna(stop_times_by_route_df['route_id'], inplace=True)
    process_end = timeit.default_timer()
    with open('../logs.txt', 'a+') as f:
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "trips_list" + " | " + str(total_time_rounded) + " seconds.", file=f)
        print("******************")
    print("Processing trip shapes...")
    process_start = timeit.default_timer()
    # Group by shape_id and create a LineString for each group
    shapes_combined_gdf = shapes_combined_gdf.groupby('shape_id').apply(lambda df: LineString(df.geometry.tolist())).reset_index()

    # Rename the 0 column to geometry
    shapes_combined_gdf.rename(columns={0: 'geometry'}, inplace=True)
    # Merge shapes_combined_gdf with trips_df
    trips_df = pd.merge(trips_df, shapes_combined_gdf, on='shape_id', how='left')

    # Create a GeoDataFrame
    trip_shapes_gdf = gpd.GeoDataFrame(trips_df, geometry='geometry')

    # Join trips_df with shapes_combined_gdf on shape_id
    joined_df = pd.merge(trips_df, shapes_combined_gdf, on='shape_id')
    trip_directions = joined_df[['trip_id', 'shape_id', 'direction_id']].copy()
    # Use direction_id from the joined_df to populate the direction_id in the trip_shapes_gdf and trip_directions
    trip_shapes_gdf = pd.merge(trip_shapes_gdf, joined_df[['shape_id', 'direction_id']], on='shape_id', how='left')
    trip_directions = pd.merge(trip_directions, joined_df[['shape_id', 'direction_id']], on='shape_id', how='left')

    # Now trip_shapes_gdf and trip_directions should have the direction_id column populated
    if debug == False:
        trip_shapes_gdf.to_postgis('trip_shapes', engine, index=False, if_exists='replace', schema=TARGET_SCHEMA)
        trip_directions.to_sql('trip_directions', engine, index=False, if_exists='replace', schema=TARGET_SCHEMA)

    with open('../logs.txt', 'a+') as f:
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "trip shapes and trip directions" + " | " + str(total_time_rounded) + " seconds.", file=f)
        print("Done processing trip shapes.")

    print("Processing trip_shape_stops_df ...")
    trip_shape_stops_df = stop_times_df.groupby(['trip_id', 'shape_id'])['stop_id'].apply(list).reset_index()
    if debug == False:
        trip_shape_stops_df.to_sql('trip_shape_stops', engine, index=False, if_exists='replace', schema=TARGET_SCHEMA)
    
    process_start = timeit.default_timer()

    # Perform the joins
    df = pd.merge(stop_times_df, trips_df, on='trip_id')
    df = pd.merge(df, route_stops_geo_data_frame, on='route_id')
    df = pd.merge(df, shapes_combined_gdf, on='shape_id')

    # Get unique route_codes
    unique_route_codes = df['route_code'].unique()

    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame()

    # Process each unique route_code
    for route_code in unique_route_codes:
        df_route_code = df[df['route_code'] == route_code]

        # Get unique direction_ids for the current route_code
        unique_direction_ids = df_route_code['direction_id'].unique()

        # Process each unique direction_id
        for direction_id in unique_direction_ids:
            df_route = df_route_code[df_route_code['direction_id'] == direction_id]

            # Sort the DataFrame
            df_route = df_route.sort_values(['service_id', 'trip_id', 'stop_sequence'])

            # Append the sorted DataFrame to the result DataFrame
            result_df = result_df.append(df_route)

    # Write the result DataFrame to a new table in the database
    if debug == False:
        result_df.to_sql('unique_shape_stop_times', engine, index=False, if_exists='replace', schema=TARGET_SCHEMA)
    process_end = timeit.default_timer()
    with open('../logs.txt', 'a+') as f:
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "unique_shape_stop_times" + " | " + str(total_time_rounded) + " seconds.", file=f)
    print("Done processing unique shape stop times.")

    # Assuming stop_times_df is your DataFrame
    max_stop_sequence_df = stop_times_df.groupby('trip_id')['stop_sequence'].max().reset_index()

    # Rename the column to 'max_stop_sequence'
    max_stop_sequence_df.rename(columns={'stop_sequence': 'max_stop_sequence'}, inplace=True)

    # Assuming max_stop_sequence_df and trips_df are GeoDataFrames with 'trip_shape' as a geometry column
    df = pd.merge(max_stop_sequence_df, trips_df, on='trip_id')

    df_max_stop_sequence = df.loc[df.groupby(['route_id', 'direction_id'])['max_stop_sequence'].idxmax()]
    df_grouped = df_max_stop_sequence.groupby(['route_id', 'direction_id'])['trip_shape'].first().reset_index()

    # Here we convert the 'trip_shape' to a LineString geometry
    df_grouped['trip_shape'] = df_grouped['trip_shape'].apply(LineString)

    # Create a new GeoDataFrame
    df_grouped = gpd.GeoDataFrame(df_grouped, geometry='trip_shape')

    # Create new geometry fields 'shape_direction_0' and 'shape_direction_1' based on the 'direction_id'
    df_grouped['shape_direction_0'] = df_grouped.apply(lambda row: row['trip_shape'] if row['direction_id'] == 0 else None, axis=1)
    df_grouped['shape_direction_1'] = df_grouped.apply(lambda row: row['trip_shape'] if row['direction_id'] == 1 else None, axis=1)

    route_overview = gpd.read_postgis('route_overview', engine, geom_col='geometry')
    route_overview_updated = route_overview.merge(df_grouped, on='route_id', how='left')

    # Make sure to replace 'your_sql_engine' with your actual SQL engine
    if debug == False:
        # save to database
        route_overview_updated.to_postgis('route_overview', engine, if_exists='replace', index=False, schema=TARGET_SCHEMA)

    print("Processing route stops...")
    process_start = timeit.default_timer()
    route_stops_geo_data_frame = gpd.GeoDataFrame(stop_times_by_route_df, geometry=stop_times_by_route_df.apply(lambda x: get_lat_long_from_coordinates(x.geojson),axis=1))
    route_stops_geo_data_frame.set_crs(epsg=4326, inplace=True)
    if debug == False:
        # save to database
        route_stops_geo_data_frame.to_postgis('route_stops',engine,index=False,if_exists="replace",schema=TARGET_SCHEMA)
    
    with open('../logs.txt', 'a+') as f:
        process_end = timeit.default_timer()
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "route_stops" + " | " + str(total_time_rounded) + " seconds.", file=f)
    print("Done processing route stops.")

def get_lat_long_from_coordinates(geojson):
    this_geojson_geom = geojson['geometry']
    return Point(this_geojson_geom['coordinates'][0], this_geojson_geom['coordinates'][1])

def create_gdf_for_shapes(temp_df_bus,temp_df_rail):
    temp_gdf_bus = gpd.GeoDataFrame(temp_df_bus, geometry=gpd.points_from_xy(temp_df_bus.shape_pt_lon, temp_df_bus.shape_pt_lat))   
    temp_gdf_rail = gpd.GeoDataFrame(temp_df_rail, geometry=gpd.points_from_xy(temp_df_rail.shape_pt_lon, temp_df_rail.shape_pt_lat))
    shapes_combined_gdf = gpd.GeoDataFrame(pd.concat([temp_gdf_bus, temp_gdf_rail],ignore_index=True),geometry='geometry')
    shapes_combined_gdf.crs = 'EPSG:4326'
    return shapes_combined_gdf

def get_stop_times_from_stop_id(this_row):
    trips_by_route_df = trips_df.loc[trips_df['route_id'] == this_row.route_id]
    stop_times_by_trip_df = stop_times_df[stop_times_df['trip_id'].isin(trips_by_route_df['trip_id'])]

    # get the stop times for this stop id
    this_stops_df = stop_times_by_trip_df.loc[stop_times_by_trip_df['stop_id'] == this_row.stop_id]
    this_stops_df = this_stops_df.sort_values(by=['departure_time'],ascending=True)
    departure_times_array = this_stops_df['departure_time'].values.tolist()
    return departure_times_array

def update_stops_seperately(temp_df_bus,temp_df_rail,file):
    temp_df_bus['agency_id'] = 'LACMTA'
    temp_gdf_bus_stops = gpd.GeoDataFrame(temp_df_bus,geometry=gpd.points_from_xy(temp_df_bus.stop_lon, temp_df_bus.stop_lat))
    temp_gdf_bus_stops.set_crs(epsg=4326, inplace=True)
    temp_df_rail['agency_id'] = 'LACMTA_Rail'
    temp_gdf_bus_stops['stop_id'] = temp_gdf_bus_stops['stop_id'].astype('str')
    temp_gdf_bus_stops['stop_code'] = temp_gdf_bus_stops['stop_code'].astype('str')
    temp_gdf_bus_stops['parent_station'] = temp_gdf_bus_stops['parent_station'].astype('str')
    temp_gdf_bus_stops['tpis_name'] = temp_gdf_bus_stops['tpis_name'].astype('str')

    temp_gdf_rail_stops = gpd.GeoDataFrame(temp_df_rail,geometry=gpd.points_from_xy(temp_df_rail.stop_lon, temp_df_rail.stop_lat))
    temp_gdf_rail_stops.set_crs(epsg=4326, inplace=True)
    temp_gdf_rail_stops['stop_id'] = temp_gdf_rail_stops['stop_id'].astype('str')
    temp_gdf_rail_stops['stop_code'] = temp_gdf_rail_stops['stop_code'].astype('str')
    temp_gdf_rail_stops['parent_station'] = temp_gdf_rail_stops['parent_station'].astype('str')
    temp_gdf_rail_stops['tpis_name'] = temp_gdf_rail_stops['tpis_name'].astype('str')
    if debug == False:
        temp_gdf_rail_stops.to_postgis("stops",engine,schema=TARGET_SCHEMA,if_exists="replace",index=False)
        temp_gdf_bus_stops.to_postgis("stops",engine,schema=TARGET_SCHEMA,if_exists="append",index=False)
    return pd.concat([temp_gdf_bus_stops,temp_gdf_rail_stops])
    

#### TRIP CREATION ####

def get_day_type_from_service_id(row):
    # print('Getting day type from service id')
    cleaned_row = str(row).lower()
    if 'weekday' in cleaned_row:
        return 'weekday'
    elif 'saturday' in cleaned_row:
        return 'saturday'
    elif 'sunday' in cleaned_row:
        return 'sunday'

def get_day_type_from_trip_id(trip_id):
    # print('Getting day type from trip id')
   this_service_id = trips_df.loc[trips_df['trip_id'] == trip_id, 'service_id'].iloc[0]
   return get_day_type_from_service_id(this_service_id)



def encode_lat_lon_to_geojson(lat,lon):
    this_geojson = {
        "type":"Feature",
        "geometry":{
            "type":"Point",
            "coordinates": [lon,lat]
        }
    }
    return this_geojson


def get_stops_data_based_on_stop_id(stop_id):
    this_stops_df = stops_df.loc[stops_df['stop_id'] == str(stop_id)]
    new_object = encode_lat_lon_to_geojson(this_stops_df['stop_lat'].values[0],this_stops_df['stop_lon'].values[0])
    return new_object


def get_stop_times_for_trip_id(this_row, stop_times_df):
    this_trips_df = stop_times_df.loc[stop_times_df['trip_id'] == this_row.trip_id]
    this_trips_df['route_id'] = this_row.route_id
    this_trips_df['direction_id'] = this_row.direction_id
    this_trips_df['day_type'] = this_row.day_type
    this_trips_df['geojson'] = this_trips_df.apply(lambda x: get_stops_data_based_on_stop_id(x.stop_id),axis=1)
    this_trips_df['stop_name'] = this_trips_df.apply(lambda x: stops_df.loc[stops_df['stop_id'] == str(x.stop_id)]['stop_name'].values[0],axis=1)
    simplified_df = this_trips_df[['route_id','route_code','stop_id','day_type','stop_sequence','direction_id','stop_name','geojson','agency_id']]
    
    df_to_combine.append(simplified_df)
    return simplified_df


def main():
    if DB_URI is None:
        print('No database URI provided.')
        sys.exit(1)
    if TARGET_SCHEMA is None:
        print('No database schema provided.')
        sys.exit(1)
    process_zip_files_for_agency_id('lacmta')
    process_zip_files_for_agency_id('lacmta-rail')
    update_gtfs_static_files()

    pass


def remove_temp_files():
    temp_directory = os.path.join(os.getcwd(),'temp')
    if os.path.exists(temp_directory):
        shutil.rmtree(temp_directory)
    else:
        print("The temp  does not exist")

def commit_to_github_repo():
    try:
        print('Committing logs to github repo')
        os.system('git add .')
        os.system('git commit -m "Updated '+str(datetime.datetime.now()))
        os.system('git push origin main')
    except Exception as e:
        print(e)
        pass

if __name__ == "__main__":
    main()
    if local == False:
        commit_to_github_repo()
    remove_temp_files()
