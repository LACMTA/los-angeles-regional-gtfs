import pandas as pd
from sqlalchemy import create_engine,MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from shapely.geometry import Point

cpdef combine_dataframes(temp_df_bus,temp_df_rail):
    return pd.concat([temp_df_bus, temp_df_rail], axis=1,sort=False)


cpdef create_list_of_trips(trips,stop_times):
    print('Creating list of trips')
    global trips_list_df
    # stop_times['day_type'] = stop_times['trip_id_event'].map(get_day_type_from_service_id)
    # stop_times['day_type'] = stop_times['day_type'].fillna(stop_times['trip_id'].map(get_day_type_from_trip_id))
    trips_list_df = stop_times.groupby('trip_id')['stop_sequence'].max().sort_values(ascending=False).reset_index()
    trips_list_df = trips_list_df.merge(stop_times[['trip_id','stop_id','stop_sequence','route_code']], on=['trip_id','stop_sequence'])
    # summarized_trips_df = trips[["route_id","trip_id","direction_id","service_id","agency_id"]]
    # summarized_trips_df['day_type'] = summarized_trips_df['service_id'].map(get_day_type_from_service_id)
    # trips_list_df = trips_list_df.merge(summarized_trips_df, on='trip_id').drop_duplicates(subset=['route_id','day_type','direction_id'])
    # trips_list_df.to_csv('trips_list_df.csv')
    return trips_list_df

cpdef update_dataframe_to_db(combined_temp_df,target_table_name,engine,target_schema):
    print('Updating dataframe to db')
    combined_temp_df.to_sql(target_table_name,engine,index=False,if_exists="replace",schema=target_schema)


