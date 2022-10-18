import pandas as pd
from pathlib import Path
import uuid
import random
import pygeohash as gh
import geopandas as gpd
import os, sys, datetime
from datetime import timezone, datetime
import pytz
from shapely import wkt
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
from dba import client
import constants_v1
import geoalchemy2
from geoalchemy2 import Geometry


def compute_uuid(seeds: str) -> uuid.UUID:
    # "calculating syncID"
    rd = random.Random()
    rd.seed(seeds)
    reproducible_seed = uuid.UUID(int=rd.getrandbits(128))
    return reproducible_seed


def compute_syncID(df):
    # "calculate seed and assign to syncID"
    df = df_all.assign(
        seeds=df[['RouteID', 'geohash']].apply(lambda row: '_'.join([str(each) for each in row]), axis=1))
    df.reindex(columns=df.columns.tolist() + ['SyncID'])
    df['SyncID'] = [compute_uuid(seed) for seed in df['seeds']]
    df['SyncID'] = df['SyncID'].values.astype(str)
    df.drop('seeds', axis=1, inplace=True)
    return df


def df_postgres():
    # "reading multiple shapefile, convert into dataframe and concatenate after it"
    #path = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\LabelBox\Batch3LB2'
    path = '/Users/parv.agarwal/Desktop/LabelBox/Batch3LB2'
    shpfiles = []
    csvfiles = []
    for dirpath, subdirs, files in os.walk(path):
        for x in files:
            if x.endswith("_poly.shp"):
                shpfiles.append(os.path.join(dirpath, x))
    sdf = pd.concat([
        gpd.read_file(shp)
        for shp in shpfiles
    ]).pipe(gpd.GeoDataFrame)

    for dirpath, subdirs, files in os.walk(path):
        for y in files:
            if y.endswith(".csv"):
                csvfiles.append(os.path.join(dirpath, y))

    # concatenate csv dataframe
    cdf = pd.concat([pd.read_csv(f) for f in csvfiles])

    mdf = sdf.merge(cdf, on='uuid', how='left')
    gdf = gpd.GeoDataFrame(mdf)

    df = gdf.to_crs('EPSG:4326')
    df.reindex(columns=df.columns.tolist() + ['lat', 'lon', 'geohash'])
    df['lon'] = df.centroid.map(lambda p: p.x)
    df['lat'] = df.centroid.map(lambda p: p.y)
    df['geohash'] = df.apply(lambda x: gh.encode(x.lat, x.lon, precision=20), axis=1)
    df['InsertDate'] = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    df['InsertDate'] = df['InsertDate'].astype('datetime64[ns]')
    my_timezone = pytz.timezone('Asia/Calcutta')
    df['InsertDate'] = df['InsertDate'].dt.tz_localize(my_timezone)
    df['InsertDate'] = pd.to_datetime(df["InsertDate"].dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    df.rename(columns={'Name': 'FeatureName', 'Status': 'FeatureStatus', 'uuid': 'UID'},
              inplace=True)
    return df


def stage1_df_postgres(df):
    # "insert dataframe into postgres"
    user = "dams_user_1"
    password = "dams_password_1"
    host = "localhost"
    port = 5434
    database = "dams_demo"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    df.to_postgis(name="Stage1Poly_L4", con=engine, if_exists='append', schema="public", index=False, chunksize=10000,
                  dtype={'geometry': Geometry('POLYGON', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE "Stage1Poly_L4" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)


def stage2_df_postgres(df):

    user = "dams_user_1"
    password = "dams_password_1"
    host = "localhost"
    port = 5434
    database = "dams_demo"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    df.to_postgis(name="Stage2Poly_L4", con=engine, if_exists='append', schema="public", index=False, chunksize=10000,
                  dtype={'geometry': Geometry('POLYGON', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE "Stage2Poly_L4" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)
    print("success")


if __name__ == "__main__":
    print("START")
    env = 'dams_demo'

    shape_dataframe = df_postgres()
    stage1_df_postgres(shape_dataframe)

    #out_path = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\LabelBox\Batch3LB\Batch3_poly.shp'
    out_path = '/Users/parv.agarwal/Desktop/LabelBox/Batch3LB/Batch3_poly.shp'
    msg = {}

    conn_dict = constants_v1.DB_CONN[env]
    db = client.DBObject(conn_dict)

    # for traffic_lights
    query = '''SELECT q."FeatureName", q."FeatureStatus", q."lat", q."lon", q."geohash",q."images", q."bboxes", 
    q."UID", q."merge_id",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."RouteMaint", 
    foo."RouteID", foo."BeginMp1", foo."EndMp1", foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", foo."RouteCla", 
    foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry 
    FROM "Stage1Poly_L4" AS q CROSS JOIN LATERAL 
(SELECT * FROM "NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''
    print("success")

    df_all = gpd.GeoDataFrame.from_postgis(query, db.connection, geom_col='geometry')
    if len(df_all) > 0:
        df_final = compute_syncID(df_all)
        cols = [col for col in df_final.columns if col != 'time_stamp']
        df_final[cols].to_file(out_path)
        stage2_df_postgres(df_final)
    else:
        print("DataFrame is Empty")
