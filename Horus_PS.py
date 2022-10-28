import pandas as pd
from pathlib import Path
import uuid
import random
import pygeohash as gh
import geopandas as gpd
import os, sys, datetime
from datetime import timezone, datetime
import pytz
from shapely import wkb
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
from dba import client
import constants_v1
import geoalchemy2
from geoalchemy2 import Geometry
from datetime import date
import boto3

class shpDataExtractor:
    def __init__(self, bucket_name, tool):
        self.tool = tool
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def list_files(self, prefix):
        """Read files to run co-registration on

        :param bucket_name:
        :param prefix:
        :return:
        """
        continuation_token = None
        while True:
            list_kwargs = dict(MaxKeys=1000000,
                               Bucket=self.bucket_name,
                               Prefix=prefix,
                               RequestPayer='requester')
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = self.s3_client.list_objects_v2(**list_kwargs)
            for content in response.get('Contents', []):
                yield content.get('Key')
            if not response.get('IsTruncated'):  # At the end of the list?
                break
            continuation_token = response.get('NextContinuationToken')

    def batch_scheduler(self, tool):
        import boto3
        from botocore.errorfactory import ClientError   
        all_files = self.list_files(f"ncdot/final_data/{tool}/shapefiles/<batch name>/<sub-batch name>/")
        all_files = sorted(all_files)
        ext = [".shp", ".shx", ".prj", ".dbf", ".cpg", ".json", ".csv"]
        all_files = [x for x in all_files if x.endswith(tuple(ext))]
        print(f"Found {len(all_files)} new SHPs in 'ncdot/final_data/{tool}/shapefiles/lz/'")
        N = len(all_files)
        print(N)
        for i in range(N):
            print(i)
            lz_list.append(all_files[i])
            os.chdir(dir_path)
            os.makedirs(f"{tool}/working/{all_files[i].split('/')[-2]}", exist_ok=True)
            os.makedirs(f"{tool}/target/", exist_ok=True)
            local_working_dir = str(dir_path)+str(tool)+'/working/'+str(all_files[i].split('/')[-2])+'/'+str(all_files[i].split('/')[-1])
            self.s3_client.download_file('aidash-rsms-main',all_files[i],local_working_dir)

def schedule_shp_extration(tool):
    """
    Trigger Mission Geotiff extraction
    Args:
    """
    bucket_name = 'aidash-rsms-main'
    scheduler = shpDataExtractor(bucket_name,tool)
    scheduler.batch_scheduler(tool)


def compute_uuid(seeds: str) -> uuid.UUID:
    # "calculating syncID"
    rd = random.Random()
    rd.seed(seeds)
    reproducible_seed = uuid.UUID(int=rd.getrandbits(128))
    return reproducible_seed


def compute_syncID(df):
    # "calculate seed and assign to syncID"
    df = df.assign(
        seeds=df[['RouteID', 'geohash']].apply(lambda row: '_'.join([str(each) for each in row]), axis=1))
    df.reindex(columns=df.columns.tolist() + ['SyncID'])
    df['SyncID'] = [compute_uuid(seed) for seed in df['seeds']]
    df['SyncID'] = df['SyncID'].values.astype(str)
    df.drop('seeds', axis=1, inplace=True)
    df.drop_duplicates(subset=['SyncID'])  # dropping duplicate data
    return df


def df_postgres(input_path):
    point_shpfiles = []
    line_shpfiles = []
    _drop_z = lambda geom: wkb.loads(wkb.dumps(geom, output_dimension=2))

    for dirpath, subdirs, files in os.walk(input_path):
        for x in files:
            if x.endswith(".shp"):
                lst = os.path.join(dirpath, x)
                try:
                    fdf = gpd.read_file(lst)
                    fdf["fpath"] = lst.replace('\\', '/').split("/")[-2]
                    fdf.geometry = fdf.geometry.transform(_drop_z)
                    if (fdf.geom_type.unique() == 'LineString'):
                        line_shpfiles.append(fdf)
                    elif (fdf.geom_type.unique()== 'Point'):
                        point_shpfiles.append(fdf)
                except:
                    print("error")

    line_ldf = pd.concat(line_shpfiles)
    line_gdf = gpd.GeoDataFrame(line_ldf)
    line_df = line_gdf.to_crs('EPSG:4326')
    line_df.reindex(columns=line_df.columns.tolist() + ['lat', 'lon', 'geohash', 'center_coord'])
    line_df['center_coord'] = line_df['geometry'].centroid
    line_df['lon'] = line_df.center_coord.map(lambda p: p.x)
    line_df['lat'] = line_df.center_coord.map(lambda p: p.y)
    line_df['geohash'] = line_df.apply(lambda x: gh.encode(x.lat, x.lon, precision=20), axis=1)
    line_df.rename(columns={'Layer_name': 'FeatureName', 'Status': 'FeatureStatus', 'Route_ID': 'FeatRouteID'},
              inplace=True)

    point_ldf = pd.concat(point_shpfiles)
    point_gdf = gpd.GeoDataFrame(point_ldf)
    point_df = point_gdf.to_crs('EPSG:4326')
    point_df.reindex(columns=point_df.columns.tolist() + ['lat', 'lon', 'geohash'])
    point_df['lon'] = point_df.centroid.map(lambda p: p.x)
    point_df['lat'] = point_df.centroid.map(lambda p: p.y)
    point_df['geohash'] = point_df.apply(lambda x: gh.encode(x.lat, x.lon, precision=20), axis=1)
    point_df.rename(columns={'Layer_name': 'FeatureName', 'Status': 'FeatureStatus', 'Route_ID': 'FeatRouteID'},
              inplace=True)
    return line_df,point_df


def stage1_point_df_postgres(df):
    # "insert dataframe into postgres"
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    df.to_postgis(name="Horus_Stage1_point", con=engine, if_exists='append', schema="ncdot", index=False,
                  dtype={'geometry': Geometry('POINT', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE ncdot."Horus_Stage1_point" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)


def stage2_point_df_postgres(df):
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    print("conn")
    df.to_postgis(name="Horus_Stage2_point", con=engine, if_exists='append', schema="ncdot", index=False,
                  dtype={'geometry': Geometry('POINT', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE ncdot."Horus_Stage2_point" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)
    print("success")


def stage1_line_df_postgres(df):
    # "insert dataframe into postgres"
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    df.to_postgis(name="Horus_Stage1_line", con=engine, if_exists='append', schema="ncdot", index=False,
                  dtype={'geometry': Geometry('LINESTRING', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE ncdot."Horus_Stage1_line" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)


def stage2_line_df_postgres(df):
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    print("conn")
    df.to_postgis(name="Horus_Stage2_line", con=engine, if_exists='append', schema="ncdot", index=False,
                  dtype={'geometry': Geometry('LINESTRING', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE ncdot."Horus_Stage2_line" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)
    print("success")


def drop_tables():
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    print("deleting table")
    with engine.connect() as con, con.begin():
        alter_query1 = '''drop table if exists ncdot."Horus_Stage1_point";'''
        print(alter_query1)
        con.execute(alter_query1)
        alter_query2 = '''drop table if exists ncdot."Horus_Stage2_point";'''
        print(alter_query2)
        con.execute(alter_query2)
        alter_query3 = '''drop table if exists ncdot."Horus_Stage1_line";'''
        print(alter_query3)
        con.execute(alter_query3)
        alter_query4 = '''drop table if exists ncdot."Horus_Stage2_line";'''
        print(alter_query4)
        con.execute(alter_query4)


if __name__ == "__main__":
    print("START")
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"
    env = 'dot'

    drop_tables()

    today = date.today()
    dt = today.strftime("%Y%m%d")
    dir_path = '/Users/parv.agarwal/Desktop/shapefiles/'
    tool = 'horus'
    lz_list =[]
    batch_name = ''
    #schedule_shp_extration(tool)
    input_path = '/Users/parv.agarwal/Downloads/division13_GIS_27102022_batch1_Horus/'
    ##########
    line_df , point_df = df_postgres(input_path)
    stage1_point_df_postgres(point_df)
    stage1_line_df_postgres(line_df)

    point_out_path = '/Users/parv.agarwal/Desktop/shapefiles/division13_GIS_27102022_batch1_Horus_Point.shp'
    msg = {}
    line_out_path = '/Users/parv.agarwal/Desktop/shapefiles/division13_GIS_27102022_batch1_Horus_Line.shp'
    msg = {}

    conn_dict = constants_v1.DB_CONN[env]
    db = client.DBObject(conn_dict)

    # for traffic_lights
    # removed attribute from point table ->  q."FeatureStatus"
    point_query = '''SELECT q."FeatureName", q."FeatRouteID", q."lat", q."lon", q."geohash", 
    q."Frame_numb", q."images", q."fpath", q."COG",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", 
    foo."Length", foo."Width", foo."Area", foo."UUID",
    foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", foo."BeginFeatu", 
    foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", foo."RouteCla", foo."RouteInv", 
    foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry 
    FROM ncdot."Horus_Stage1_point" AS q CROSS JOIN LATERAL 
(SELECT * FROM ncdot."NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''
    line_query = '''SELECT q."FeatureName", q."FeatureStatus", q."FeatRouteID", q."lat", q."lon", q."geohash", 
    q."Frame_numb", q."images", q."fpath", q."COG",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", 
    foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", 
    foo."RouteCla", foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry FROM 
    ncdot."Horus_Stage1_line" AS q CROSS JOIN LATERAL 
(SELECT * FROM ncdot."NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''
    print("success")

    df_point_all = gpd.GeoDataFrame.from_postgis(point_query, db.connection, geom_col='geometry')
    df_line_all = gpd.GeoDataFrame.from_postgis(line_query, db.connection, geom_col='geometry')
    
    if len(df_point_all) > 0:
        df_point_final = compute_syncID(df_point_all)
        cols = [col for col in df_point_final.columns if col != 'time_stamp']
        df_point_final.to_file(point_out_path)
        #stage2_point_df_postgres(df_point_final)
    else:
        print("Point DataFrame is Empty")

    if len(df_line_all) > 0:
        df_line_final = compute_syncID(df_line_all)
        cols = [col for col in df_line_final.columns if col != 'time_stamp']
        df_line_final.to_file(line_out_path)
        #stage2_line_df_postgres(df_line_final)
    else:
        print("Line DataFrame is Empty")