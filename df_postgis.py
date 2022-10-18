import pandas as pd
import geopandas as gpd
import os, sys, datetime
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
from dba import client
import constants_v1
import pytz

# from dba import client
# from constants import *

# sys.path.insert(0, '/Users/kanishkvarshney/Desktop/Workspace/Code/ncdot-aidash/dba')
# sys.path.insert(0, '/Users/kanishkvarshney/Desktop/Workspace/Code/ncdot-aidash')

if __name__ == "__main__":
    print("START")
    start_time = datetime.datetime.now()
    # shp_path = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\NCDOT_Ref_Data\Postgres_POC\NC_WGS_UID.shp'
    NCDOT_file = r'C:\Users\ManishaAggarwal\Downloads\NCRoute_WGS_updated\NCRoute_WGS_updated\NC_WGS_Final.shp'
    # stage2 = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\SampleData\40lms_Sample_data\horus\TMX7316081502-000336\output.shp'

    msg = {}

    # old_infra_dict = constants_v1.DB_CONN['dams_demo']
    # old_infra_db = client.DBObject(old_infra_dict)

    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    print("conn")
    shp_df = gpd.read_file(NCDOT_file)
    shp_df.to_crs('EPSG:4326')
    # shp_df.rename(columns={'length': 'Length', 'width': 'Width', 'area': 'Area', 'uuid': 'UUID'}, inplace=True)
    # shp_df['UpdateDate'] = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    # shp_df['UpdateDate'] = shp_df['UpdateDate'].astype('datetime64[ns]')
    # my_timezone = pytz.timezone('Asia/Calcutta')
    # shp_df['UpdateDate'] = shp_df['UpdateDate'].dt.tz_localize(my_timezone)
    # shp_df['UpdateDate'] = pd.to_datetime(shp_df["UpdateDate"].dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    shp_df.to_postgis(name="NC_REF_DATA", con=engine, schema="ncdot", index=False)
    print(shp_df.columns)

    # old_infra_db.upsert_many(customer, 'ncdot_data', shp, ['index'])
    # table_name, self.engine, schema = schema_name, index = False, if_exists = 'append'
