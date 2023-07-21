import sys, argparse

# from calendar import calendar
import pandas as pd
import json
import os
# from pathlib import Path
# from sqlalchemy import create_engine
import geopandas as gpd
from io import StringIO
import zipfile
import timeit

from sqlalchemy import create_engine,MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from shapely.geometry import Point


debug = False
# from sqlalchemy.orm import Session,sessionmaker
# from config import Config
# from .database_connector import *

list_of_gtfs_static_files = ["routes", "trips", "stops", "calendar", "shapes","stop_times"]

# Argument parser for database connections
parser = argparse.ArgumentParser(description='Process database URI.')
parser.add_argument('--db_uri', metavar='db_uri', type=str, nargs='+',
                    help='The postgresql database URI for updating the GTFS Static data to.', required=True)

parser.add_argument('--db_schema', metavar='db_schema', type=str, nargs='+',help='Target postgresql database schema for updating.', required=True)


args = parser.parse_args()
DB_URI = args.db_uri[0]
TARGET_SCHEMA = args.db_schema[0]
engine = create_engine(DB_URI, echo=False)
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


def get_latest_modified_zip_file(path,folder_branch):
    target_path = path +"/" + folder_branch
    if path is None:
        print('No path provided.')
        sys.exit(1)
    try:
        return max([target_path+'/'+f for f in os.listdir(target_path) if f.endswith('.zip')], key=os.path.getmtime)
    except Exception as e:
        print('Error getting latest modified zip file: ' + str(e))
        sys.exit(1)




def process_zip_files_for_agency_id(agency_id):
    target_zip_files = None
    if agency_id is None:
        print('No agency_id provided.')
        sys.exit(1)
    if agency_id == 'lacmta':
        target_zip_files = get_latest_modified_zip_file(r'./lacmta/', 'future')
    if agency_id == 'lacmta-rail':
        target_zip_files = get_latest_modified_zip_file(r'./lacmta-rail/', 'current')
    extract_zip_file_to_temp_directory(target_zip_files,agency_id)

def extract_zip_file_to_temp_directory(zip_file,agency_id):
    try:
        print('Extracting zip file to temp directory: ' + zip_file)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall('./temp/'+agency_id)
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

        bus_file_path = "./temp/lacmta/" + file + '.txt'
        rail_file_path = "./temp/lacmta-rail/" + file + '.txt'
        temp_df_bus = pd.read_csv(bus_file_path)
        temp_df_bus['agency_id'] = 'LACMTA'
        temp_df_rail = pd.read_csv(rail_file_path)
        temp_df_rail['agency_id'] = 'LACMTA_Rail'




        if file == "stops":
            # pass
            stops_df = update_stops_seperately(temp_df_bus,temp_df_rail,file)
            # temp_gdf_bus = gpd.GeoDataFrame(temp_df_bus, geometry=gpd.points_from_xy(temp_df_bus.stop_lon, temp_df_bus.stop_lat))
            # temp_gdf_rail = gpd.GeoDataFrame(temp_df_rail, geometry=gpd.points_from_xy(temp_df_rail.stop_lon, temp_df_rail.stop_lat))
            # stops_combined_gdf = gpd.GeoDataFrame(pd.concat([temp_gdf_bus, temp_gdf_rail], ignore_index=True),geometry='geometry')
            # stops_combined_gdf.crs = 'EPSG:4326'
            # stops_combined_gdf.to_postgis(file,engine,schema=TARGET_SCHEMA,if_exists="replace",index=False)
            # stops_df = stops_combined_gdf
        elif file == "shapes":
            temp_gdf_bus = gpd.GeoDataFrame(temp_df_bus, geometry=gpd.points_from_xy(temp_df_bus.shape_pt_lon, temp_df_bus.shape_pt_lat))   
            temp_gdf_rail = gpd.GeoDataFrame(temp_df_rail, geometry=gpd.points_from_xy(temp_df_rail.shape_pt_lon, temp_df_rail.shape_pt_lat))
            shapes_combined_gdf = gpd.GeoDataFrame(pd.concat([temp_gdf_bus, temp_gdf_rail],ignore_index=True),geometry='geometry')
            shapes_combined_gdf.crs = 'EPSG:4326'
            shapes_combined_gdf.to_postgis(file,engine,index=False,if_exists="replace",schema=TARGET_SCHEMA)
        else:
            combined_temp_df = pd.concat([temp_df_bus, temp_df_rail])
            if file == "stop_times":
                stop_times_df = combined_temp_df
            if file == "trips":
                trips_df = combined_temp_df
            if file == "calendar_dates":
                calendar_dates_df = combined_temp_df
            if file == "calendar":
                calendar_df = combined_temp_df
            if debug == False:
                combined_temp_df.to_sql(file,engine,index=False,if_exists="replace",schema=TARGET_SCHEMA)
        process_end = timeit.default_timer()
        
        with open('logs.txt', 'a+') as f:
            human_readable_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_time = process_end - process_start
            total_time_rounded = round(total_time,2)
            print(human_readable_date+" | " + file + " | " + str(total_time_rounded) + " seconds.", file=f)
            print("Done with " + file)
            print("******************")
    create_list_of_trips(trips_df,stop_times_df)


    trips_list_df.apply(lambda row: get_stop_times_for_trip_id(row), axis=1)
    stop_times_by_route_df = pd.concat(df_to_combine)
    stop_times_by_route_df['departure_times'] = stop_times_by_route_df.apply(lambda row: get_stop_times_from_stop_id(row),axis=1)
    stop_times_by_route_df['route_code'].fillna(stop_times_by_route_df['route_id'], inplace=True)

    route_stops_geo_data_frame = gpd.GeoDataFrame(stop_times_by_route_df, geometry=stop_times_by_route_df.apply(lambda x: get_lat_long_from_coordinates(x.geojson),axis=1))
    route_stops_geo_data_frame.set_crs(epsg=4326, inplace=True)
    if debug == False:
        # save to database
        route_stops_geo_data_frame.to_postgis('route_stops',engine,index=False,if_exists="replace",schema=TARGET_SCHEMA)

def get_lat_long_from_coordinates(geojson):
    this_geojson_geom = geojson['geometry']
    return Point(this_geojson_geom['coordinates'][0], this_geojson_geom['coordinates'][1])



def get_stop_times_from_stop_id(this_row):
    # print('Getting stop times for stop id')
    trips_by_route_df = trips_df.loc[trips_df['route_id'] == this_row.route_id]
    
    stop_times_by_trip_df = stop_times_df[stop_times_df['trip_id'].isin(trips_by_route_df['trip_id'])]

    # get the stop times for this stop id
    this_stops_df = stop_times_by_trip_df.loc[stop_times_by_trip_df['stop_id'] == this_row.stop_id]
    this_stops_df = this_stops_df.sort_values(by=['departure_time'],ascending=True)
    # simplified_this_stops_df = simplified_this_stops_df.to_json(orient='records')

    departure_times_array = this_stops_df['departure_time'].values.tolist()
    # to check:
    # print(simplified_this_stops_df)

    # combined_stop_times_array.append(simplified_this_stops_df)
    return departure_times_array
import json
import datetime


def update_stops_seperately(temp_df_bus,temp_df_rail,file):
    # temp_df_bus['geometry'] = [Point(xy) for xy in zip(temp_df_bus.stop_lon, temp_df_bus.stop_lat)] 
    temp_df_bus['agency_id'] = 'LACMTA'
    temp_gdf_bus_stops = gpd.GeoDataFrame(temp_df_bus,geometry=gpd.points_from_xy(temp_df_bus.stop_lon, temp_df_bus.stop_lat))
    temp_gdf_bus_stops.set_crs(epsg=4326, inplace=True)

    # temp_df_rail['geometry'] = [Point(xy) for xy in zip(temp_df_rail.stop_lon, temp_df_rail.stop_lat)] 
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

def create_list_of_trips(trips,stop_times):
    print('Creating list of trips')
    global trips_list_df
    # stop_times['day_type'] = stop_times['trip_id_event'].map(get_day_type_from_service_id)
    # stop_times['day_type'] = stop_times['day_type'].fillna(stop_times['trip_id'].map(get_day_type_from_trip_id))
    trips_list_df = stop_times.groupby('trip_id')['stop_sequence'].max().sort_values(ascending=False).reset_index()
    trips_list_df = trips_list_df.merge(stop_times[['trip_id','stop_id','stop_sequence','route_code']], on=['trip_id','stop_sequence'])
    summarized_trips_df = trips[["route_id","trip_id","direction_id","service_id","agency_id"]]
    summarized_trips_df['day_type'] = summarized_trips_df['service_id'].map(get_day_type_from_service_id)
    trips_list_df = trips_list_df.merge(summarized_trips_df, on='trip_id').drop_duplicates(subset=['route_id','day_type','direction_id'])
    trips_list_df.to_csv('trips_list_df.csv')


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
    # print('Getting stops data based on stop id')
    this_stops_df = stops_df.loc[stops_df['stop_id'] == str(stop_id)]
    # print(this_stops_df[['stop_name','stop_lat','stop_lon']])
    # new_object = this_stops_df[['stop_name','stop_lat','stop_lon']].to_dict('records')
    new_object = encode_lat_lon_to_geojson(this_stops_df['stop_lat'].values[0],this_stops_df['stop_lon'].values[0])
    # print('stop_id',stop_id)
    return new_object


def get_stop_times_for_trip_id(this_row):
    this_trips_df = stop_times_df.loc[stop_times_df['trip_id'] == this_row.trip_id]
    this_trips_df['route_id'] = this_row.route_id
    # this_trips_df['service_id'] = this_row.service_id
    this_trips_df['direction_id'] = this_row.direction_id
    this_trips_df['day_type'] = this_row.day_type
    this_trips_df['geojson'] = this_trips_df.apply(lambda x: get_stops_data_based_on_stop_id(x.stop_id),axis=1)
    this_trips_df['stop_name'] = this_trips_df.apply(lambda x: stops_df.loc[stops_df['stop_id'] == str(x.stop_id)]['stop_name'].values[0],axis=1)
    # simplified_df = this_trips_df[['route_id','stop_id','service_id','day_type','direction_id','stop_name','coordinates']]
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

if __name__ == "__main__":
    main()