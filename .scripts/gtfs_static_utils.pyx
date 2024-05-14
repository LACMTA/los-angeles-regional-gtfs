import pandas as pd
from sqlalchemy import create_engine,MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from shapely.geometry import Point

def combine_dataframes(temp_df_bus,temp_df_rail):
    return pd.concat([temp_df_bus, temp_df_rail])


def create_list_of_trips(trips,stop_times):
    print('Creating list of trips')
    global trips_list_df
    trips_list_df = stop_times.groupby('trip_id')['stop_sequence'].max().sort_values(ascending=False).reset_index()
    trips_list_df = trips_list_df.merge(stop_times[['trip_id','stop_id','stop_sequence','route_code']], on=['trip_id','stop_sequence'])
    return trips_list_df

def update_dataframe_to_db(combined_temp_df,target_table_name,engine,target_schema):
    print('Updating dataframe to db')
    combined_temp_df.to_sql(target_table_name,engine,index=False,if_exists="replace",schema=target_schema)


