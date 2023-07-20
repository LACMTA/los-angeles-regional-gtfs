from calendar import calendar
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine
# from sqlalchemy.orm import Session,sessionmaker
from config import Config
import geopandas as gpd
from .database_connector import *
import requests
from io import StringIO
# from .utils.log_helper import *
# engine = create_engine(Config.DB_URI, echo=False,executemany_mode="values")
# Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

CALENDAR_DATES_URL_BUS = 'https://gitlab.com/LACMTA/gtfs_bus/-/raw/weekly-updated-service/calendar_dates.txt'
# CALENDAR_DATES_URL_RAIL = 'https://gitlab.com/LACMTA/gtfs_rail/-/raw/weekly-updated-service/calendar_dates.txt'
CALENDAR_DATES_URL_RAIL = 'https://gitlab.com/LACMTA/gtfs_rail/-/raw/master/calendar_dates.txt'
# session = Session()

list_of_gtfs_static_files = ["agency","routes", "trips", "stops", "calendar", "shapes"]
# list_of_gtfs_static_files = ["routes", "trips", "stop_times", "stops", "calendar", "shapes"]

def update_calendar_dates():
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"}
    req = requests.get(CALENDAR_DATES_URL_BUS, headers=headers)
    calendar_dates_bus_data = StringIO(req.text)
    req = requests.get(CALENDAR_DATES_URL_RAIL, headers=headers)
    calendar_dates_rail_data = StringIO(req.text)
    calendar_dates_df_bus = pd.read_csv(calendar_dates_bus_data)
    calendar_dates_df_bus['agency_id'] = 'LACMTA'
    calendar_dates_df_rail = pd.read_csv(calendar_dates_rail_data)
    calendar_dates_df_rail['agency_id'] = 'LACMTA_Rail'
    calendar_dates_df = pd.concat([calendar_dates_df_bus, calendar_dates_df_rail])
    calendar_dates_df.to_sql('calendar_dates',engine,index=True,if_exists="replace",schema=Config.TARGET_DB_SCHEMA)

def update_gtfs_static_files():
    for file in list_of_gtfs_static_files:
        bus_file_path = "../../appdata/gtfs-static/gtfs_bus/" + file + '.txt'
        rail_file_path = "../../gtfs-static/gtfs_rail/" + file + '.txt'
        temp_df_bus = pd.read_csv(bus_file_path)
        temp_df_bus['agency_id'] = 'LACMTA'
        temp_df_rail = pd.read_csv(rail_file_path)
        temp_df_rail['agency_id'] = 'LACMTA_Rail'
        if file == "stops":
            temp_gdf_bus = gpd.GeoDataFrame(temp_df_bus, geometry=gpd.points_from_xy(temp_df_bus.stop_lon, temp_df_bus.stop_lat))
            temp_gdf_rail = gpd.GeoDataFrame(temp_df_rail, geometry=gpd.points_from_xy(temp_df_rail.stop_lon, temp_df_rail.stop_lat))
            combined_gdf = gpd.GeoDataFrame(pd.concat([temp_gdf_bus, temp_gdf_rail], ignore_index=True),geometry='geometry')
            stops_combined_gdf.crs = 'EPSG:4326'
            stops_combined_gdf.to_postgis(file,engine,schema=Config.TARGET_DB_SCHEMA,if_exists="replace",index=False)
        if file == "shapes":
            temp_gdf_bus = gpd.GeoDataFrame(temp_df_bus, geometry=gpd.points_from_xy(temp_df_bus.shape_pt_lon, temp_df_bus.shape_pt_lat))   
            temp_gdf_rail = gpd.GeoDataFrame(temp_df_rail, geometry=gpd.points_from_xy(temp_df_rail.shape_pt_lon, temp_df_rail.shape_pt_lat))
            shapes_combined_gdf = gpd.GeoDataFrame(pd.concat([temp_gdf_bus, temp_gdf_rail],ignore_index=True),geometry='geometry')
            shapes_combined_gdf.crs = 'EPSG:4326'
            shapes_combined_gdf.to_postgis(file,engine,index=False,if_exists="replace",schema=Config.TARGET_DB_SCHEMA)

        else:
            combined_temp_df = pd.concat([temp_df_bus, temp_df_rail])
            combined_temp_df.to_sql(file,engine,index=False,if_exists="replace",schema=Config.TARGET_DB_SCHEMA)
