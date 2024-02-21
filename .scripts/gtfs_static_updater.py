import sys, argparse

# from calendar import calendar
import os

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
from pathlib import Path
from shapely import wkt

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
route_overview_file_location = "../.scripts/route_overview.csv"

def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


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

def combine_dataframes(temp_df_bus,temp_df_rail):
    return pd.concat([temp_df_bus, temp_df_rail])

def create_list_of_trips(trips,stop_times):
    print('Creating list of trips')
    global trips_list_df
    trips_list_df = stop_times.groupby('trip_id')['stop_sequence'].max()
    trips_list_df.sort_values(ascending=False, inplace=True)
    trips_list_df.reset_index(inplace=True)
    stop_times_subset = stop_times[['trip_id','stop_id','stop_sequence','route_code']].set_index(['trip_id','stop_sequence'])
    trips_list_df = trips_list_df.join(stop_times_subset, on=['trip_id','stop_sequence'])
    return trips_list_df

def update_dataframe_to_db(combined_temp_df,target_table_name,engine,target_schema):
    print('Updating dataframe to db')
    combined_temp_df.to_sql(target_table_name,engine,index=False,if_exists="replace",schema=target_schema)
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

    trips_list_df.apply(lambda row: get_stop_times_for_trip_id(row), axis=1)
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
    print("Done processing trip list.")
    print("Processing trip stop times...")

    # Assuming stop_times_df already contains 'trip_id', 'stop_id', 'stop_sequence', 'stop_name', etc.
    # We just need to drop duplicates and set the index
    stop_times_df.drop_duplicates(subset=['trip_id', 'stop_id'], inplace=True)

    # Write the DataFrame to a new table in the PostgreSQL database
    if debug == False:
        stop_times_df.to_sql('trip_stop_times', engine, if_exists='replace', index=True, schema=TARGET_SCHEMA)
    with open('../logs.txt', 'a+') as f:
        process_end = timeit.default_timer()
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "trip_stop_times" + " | " + str(total_time_rounded) + " seconds.", file=f)
        print("******************")
    print("Done processing trip stop times.")
    print("Processing trip shapes...")
    process_start = timeit.default_timer()
    trip_shapes_df = shapes_combined_gdf.groupby(['shape_id', 'agency_id'])['geometry'].apply(lambda x: LineString(x.tolist())).reset_index()
    if debug == False:
        trip_shapes_df.to_postgis('trip_shapes', engine, if_exists='replace', index=False, schema=TARGET_SCHEMA)
    process_end = timeit.default_timer()
    with open('../logs.txt', 'a+') as f:
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "trip_shapes" + " | " + str(total_time_rounded) + " seconds.", file=f)
        print("******************")
    print("Done processing trip shapes.")

    print("Processing route overview...")
    # Read the existing route_overview table
    route_overview = pd.read_csv(route_overview_file_location)

    # Update the route_overview table in the database
    if debug == False:
        route_overview.to_sql('route_overview', engine, if_exists='replace', index=False, schema=TARGET_SCHEMA)

    process_end = timeit.default_timer()

    with open('../logs.txt', 'a+') as f:
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "route_overview" + " | " + str(total_time_rounded) + " seconds.", file=f)
        print("******************")
    print("Done processing route overview.")

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

    print("Processing route stops grouped...")
    route_stops_grouped = route_stops_geo_data_frame.groupby(['route_code', 'agency_id']).apply(process_group).reset_index()

    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(route_stops_grouped, geometry='shape_direction_0')

    if debug == False:
        # save to database
        gdf.to_postgis('route_stops_grouped', engine, index=False, if_exists="replace", schema=TARGET_SCHEMA)
    with open('../logs.txt', 'a+') as f:
        process_end = timeit.default_timer()
        human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = process_end - process_start
        total_time_rounded = round(total_time,2)
        print(human_readable_date+" | " + "route_stops_grouped" + " | " + str(total_time_rounded) + " seconds.", file=f)
        print("******************")
    print("Done processing route stops grouped.")

def process_group(group):
    payload = group.to_dict('records')
    shape_direction_0 = group[group['direction_id'] == 0]['geometry'].unary_union
    shape_direction_1 = group[group['direction_id'] == 1]['geometry'].unary_union
    return pd.Series({'payload': payload, 'shape_direction_0': shape_direction_0, 'shape_direction_1': shape_direction_1})

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

    # filter for pickup_type = 0 and drop_off_type = 0
    stop_times_by_trip_df = stop_times_by_trip_df[(stop_times_by_trip_df['pickup_type'] == 0) & (stop_times_by_trip_df['drop_off_type'] == 0)]

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


def get_stop_times_for_trip_id(this_row):
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
