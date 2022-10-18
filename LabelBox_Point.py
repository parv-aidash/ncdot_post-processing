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
import psycopg2 as ps
from sqlalchemy import create_engine
from psycopg2 import Error
from dba import client
import constants_v1
import geoalchemy2
from geoalchemy2 import Geometry
import boto3
from datetime import date
import shutil
import pci_calculation as pci


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
        all_files = self.list_files(f"ncdot/final_data/{tool}/shapefiles/batch3/mitchell_GIS_10112022_batch3_LB/")
        all_files = sorted(all_files)
        ext = [".shp", ".shx", ".prj", ".dbf", ".cpg", ".json", ".csv"]
        all_files = [x for x in all_files if x.endswith(tuple(ext))]
        print(f"Found {len(all_files)} new SHPs in 'ncdot/final_data/{tool}/shapefiles/lz/'")
        N = len(all_files)
        print(N)
        for i in range(N):
            #print(all_files[i])
            print(i)
            lz_list.append(all_files[i])
            #s3 = boto3.resource('s3')
            #copy_source = {
            #    'Bucket': 'aidash-rsms-main',
            #    'Key': all_files[i]
            #    }
            #working_path =  f'ncdot/final_data/{tool}/shapefiles/working/'+str(all_files[i].split('/')[-2])+'/'+str(all_files[i].split('/')[-1])
            #s3.meta.client.copy(copy_source, 'aidash-rsms-main', working_path)
            os.chdir(dir_path)
            os.makedirs(f"{tool}/working/{all_files[i].split('/')[-2]}", exist_ok=True)
            os.makedirs(f"{tool}/target/", exist_ok=True)
            local_working_dir = str(dir_path)+str(tool)+'/working/'+str(all_files[i].split('/')[-2])+'/'+str(all_files[i].split('/')[-1])
            self.s3_client.download_file('aidash-rsms-main',all_files[i],local_working_dir)
###################################################### mein boldu firse beechme
#df block

def schedule_shp_extration(tool):
    """
    Trigger Mission Geotiff extraction
    Args:
    """
    bucket_name = 'aidash-rsms-main'
    scheduler = shpDataExtractor(bucket_name,tool)
    scheduler.batch_scheduler(tool)

#def lambda_handler(event, context):
#    schedule_shp_extration(tool)
def connect_to_db(host,database,port,user,password):
    try:
        conn = ps.connect(
        host=host,
        database=database,
        port=port,
        user=user,
        password=password)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected')
        return conn

#def delete_point_table(curr):
#    delete_command = ('''Drop table ncdot."point_test1"''')
#    curr.execute(delete_command)
#
#def delete_poly_table(curr):
#    delete_command = ('''Drop table ncdot."poly_test1"''')
#    curr.execute(delete_command)


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


def point_df_postgres():
    # "reading multiple shapefile, convert into dataframe and concatenate after it"
    path = dir_path + tool +'/working'
    point_shpfiles = []
    for dirpath, subdirs, files in os.walk(path):
        for x in files:
            if x.endswith("_point.shp"):
                lst = os.path.join(dirpath, x)
                # print(lst)
                try:
                    fdf = gpd.read_file(lst)
                    fdf["fpath"] = lst.replace('\\', '/').split("/")[-2]
                    point_shpfiles.append(fdf)
                except:
                    print("error")
                
    df = pd.concat(point_shpfiles)
    print('done with df')

    df.reindex(columns=df.columns.tolist() + ['lat', 'lon', 'geohash'])
    print(df)
    df['lon'] = df.centroid.map(lambda p: p.x)
    df['lat'] = df.centroid.map(lambda p: p.y)
    df['geohash'] = df.apply(lambda x: gh.encode(x.lat, x.lon, precision=20), axis=1)
    df['InsertDate'] = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    df['InsertDate'] = df['InsertDate'].astype('datetime64[ns]')
    my_timezone = pytz.timezone('Asia/Calcutta')
    df['InsertDate'] = df['InsertDate'].dt.tz_localize(my_timezone)
    df['InsertDate'] = pd.to_datetime(df["InsertDate"].dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    df.rename(columns={'Name': 'FeatureName', 'Status': 'FeatureStatus'},
              inplace=True)
    return df

def poly_df_postgres():
    # "reading multiple shapefile, convert into dataframe and concatenate after it"
    #path = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\LabelBox\Batch3LB2'
    path = dir_path + tool +'/working'
    tgt_path = dir_path + tool +'/target'
    poly_shpfiles = []
    csvfiles = []
    for dirpath, subdirs, files in os.walk(path):
        for x in files:
            if x.endswith("_poly.shp"):
                lst = os.path.join(dirpath, x)
                # print(lst)
                try:
                    fdf = gpd.read_file(lst)
                    fdf["fpath"] = lst.replace('\\', '/').split("/")[-2]
                    poly_shpfiles.append(fdf)
                except:
                    print("error")
                
    sdf = pd.concat(poly_shpfiles)
    print('done with sdf')

    for dirpath, subdirs, files in os.walk(path):
        for y in files:
            if y.endswith(".csv"):
                csvfiles.append(os.path.join(dirpath, y))
                

    # concatenate csv dataframe
    cdf = pd.concat([pd.read_csv(f) for f in csvfiles])
    local_working_dir_file = (tgt_path+'/abc.csv')
    cdf.to_csv(local_working_dir_file, sep=',')

    mdf = sdf.merge(cdf, on='uuid', how='left')
    gdf = gpd.GeoDataFrame(mdf)

    df = gdf.to_crs('EPSG:4326')
    df.reindex(columns=df.columns.tolist() + ['lat', 'lon', 'geohash'])
    df['lon'] = df.centroid.map(lambda p: p.x)
    df['lat'] = df.centroid.map(lambda p: p.y)
    df['geohash'] = df.apply(lambda x: gh.encode(x.lat, x.lon, precision=20), axis=1)
    #df['InsertDate'] = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    #df['InsertDate'] = df['InsertDate'].astype('datetime64[ns]')
    #my_timezone = pytz.timezone('Asia/Calcutta')
    #df['InsertDate'] = df['InsertDate'].dt.tz_localize(my_timezone)
    #df['InsertDate'] = pd.to_datetime(df["InsertDate"].dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    df.rename(columns={'Name': 'FeatureName', 'Status': 'FeatureStatus', 'uuid': 'UID'},
              inplace=True)
    return df

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
        alter_query1 = '''drop table if exists ncdot."point_test1";'''
        print(alter_query1)
        con.execute(alter_query1)
        alter_query2 = '''drop table if exists ncdot."poly_test1";'''
        print(alter_query2)
        con.execute(alter_query2)
        alter_query3 = '''drop table if exists ncdot."point_test2";'''
        print(alter_query3)
        con.execute(alter_query3)
        alter_query4 = '''drop table if exists ncdot."poly_test2";'''
        print(alter_query4)
        con.execute(alter_query4)


def stage1_point_df_postgres(df):
    # "insert dataframe into postgres"
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    df.to_postgis(name="point_test1", con=engine, if_exists='append', schema="ncdot", index=False,
                  dtype={'geometry': Geometry('POINT', srid=4326)})
    
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE ncdot."point_test1" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)

def stage1_poly_df_postgres(df):
    # "insert dataframe into postgres"
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    df.to_postgis(name="poly_test1", con=engine, if_exists='append', schema="ncdot", index=False, chunksize=10000,
                  dtype={'geometry': Geometry('POLYGON', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query = '''UPDATE ncdot."poly_test1" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query)
        con.execute(alter_query)

def stage2_df_postgres(df_point,df_poly):

    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    print("conn")
    df_point.to_postgis(name="point_test2", con=engine, if_exists='append', schema="ncdot", index=False,
                  dtype={'geometry': Geometry('POINT', srid=4326)})
    df_poly.to_postgis(name="poly_test2", con=engine, if_exists='append', schema="ncdot", index=False, chunksize=10000,
                  dtype={'geometry': Geometry('POLYGON', srid=4326)})
    print("success")
    with engine.connect() as con, con.begin():
        alter_query_point = '''UPDATE ncdot."point_test2" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query_point)
        con.execute(alter_query_point)
        alter_query_poly = '''UPDATE ncdot."poly_test2" SET geometry  = ST_SetSRID(geometry, 4326);'''
        print(alter_query_poly)
        con.execute(alter_query_poly)
    print("success")


if __name__ == "__main__":
    drop_tables()
    host="localhost"
    database="dot"
    port="5435"
    user="postgres"
    password="VQaPo4AapgXbYlKb"

    today = date.today()
    dt = today.strftime("%Y%m%d")
    dir_path = '/Users/parv.agarwal/Desktop/shapefiles/'
    tool = 'labelbox'
    env = 'dot'
    lz_list =[]
    batch_name = 'mitchell_GIS_10112022_batch3_LB'
    schedule_shp_extration(tool)
    print("START")
    
    poly_shape_dataframe = poly_df_postgres()
    point_shape_dataframe = point_df_postgres()
#####

    stage1_point_df_postgres(point_shape_dataframe)
    print('Point stage 1 done')
    stage1_poly_df_postgres(poly_shape_dataframe)
    print('Poly stage 1 done')
    
    os.chdir(dir_path)
    
    #wilson_GIS_15092022_batch1_LB_polyI
    point_target_shp_name = batch_name+'_point.shp'
    point_target_shp_file_folder = batch_name+'_point'
    poly_target_shp_name = batch_name+'_poly.shp'
    poly_target_shp_file_folder = batch_name+'_LB_poly'
    poly_out_path_temp = dir_path+tool+'/target/'+poly_target_shp_file_folder+'/temp/'+poly_target_shp_name
    point_out_path = dir_path+tool+'/target/'+point_target_shp_file_folder+'/'+point_target_shp_name
    poly_out_path = dir_path+tool+'/target/'+poly_target_shp_file_folder+'/'+poly_target_shp_name

    os.makedirs(f"{tool}/target/{point_target_shp_file_folder}", exist_ok=True)
    os.makedirs(f"{tool}/target/{poly_target_shp_file_folder}/temp", exist_ok=True)
    shutil.move((dir_path + tool +'/target/abc.csv'), (dir_path+tool+'/target/'+poly_target_shp_file_folder+'/' + poly_target_shp_file_folder +'.csv'))    
    msg = {}

    conn_dict = constants_v1.DB_CONN[env]
    db = client.DBObject(conn_dict)
    point_query = '''SELECT q."FeatureName", q."FeatureStatus", q."Condition", q."Visible", q."Legible", q."Reflective", 
    q."images", q."bboxes", q."lat", q."lon", q."geohash",q."fpath",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", 
    foo."Length", foo."Width", foo."Area", foo."UUID",
    foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", foo."BeginFeatu", 
    foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", foo."RouteCla", foo."RouteInv", 
    foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry
    FROM ncdot."point_test1" AS q CROSS JOIN LATERAL 
(SELECT * FROM ncdot."NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''
    
    poly_query = '''SELECT q."FeatureName", q."FeatureStatus", q."lat", q."lon", q."geohash",q."images", q."bboxes", 
    q."UID", q."merge_id",q."fpath",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."RouteMaint", 
    foo."RouteID", foo."BeginMp1", foo."EndMp1", foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", foo."RouteCla", 
    foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry
    FROM ncdot."poly_test1" AS q CROSS JOIN LATERAL 
(SELECT * FROM ncdot."NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''
    print("success")
#, foo."UniqueID"
    point_df_all = gpd.GeoDataFrame.from_postgis(point_query, db.connection, geom_col='geometry')
    poly_df_all = gpd.GeoDataFrame.from_postgis(poly_query, db.connection, geom_col='geometry')
    print(point_df_all)
    print(poly_df_all)
    if len(point_df_all) > 0 or len(poly_df_all) > 0:
        point_df_final = compute_syncID(point_df_all)
        poly_df_final = compute_syncID(poly_df_all)
        point_cols = [col for col in point_df_final.columns if col != 'time_stamp']
        point_df_final[point_cols].to_file(point_out_path)
        poly_cols = [col for col in poly_df_final.columns if col != 'time_stamp']
        poly_df_final[poly_cols].to_file(poly_out_path_temp)
        
        poly_stg_df = gpd.read_file(poly_out_path_temp)
        final_df = poly_stg_df.drop_duplicates(subset=['SyncID','FeatureNam','FeatureSta','images','bboxes'])
        final_df['Area_damage'] = final_df['geometry'].to_crs('epsg:32633').map(lambda p: p.area)
        os.chdir(dir_path+tool+'/target/'+poly_target_shp_file_folder+'/temp')
        shutil.rmtree('temp', ignore_errors=True)
        final_df.to_file(poly_out_path)

        #PCI calculation
        pci.main(poly_out_path)
        # enumerate local files recursively
        bucket = 'aidash-rsms-main'
        destination = f'ncdot/final_data/{tool}/shapefiles/target/' + str(dt) + '/'

        client = boto3.client('s3')
        tg_path = str(dir_path)+str(tool)+'/target/'
        for root, dirs, files in os.walk(tg_path):

          for filename in files:

            # construct the full local path
            local_path = os.path.join(root, filename)
            print(local_path)

            # construct the full Dropbox path
            relative_path = os.path.relpath(local_path, tg_path)
            s3_path = os.path.join(destination, relative_path)

            # relative_path = os.path.relpath(os.path.join(root, filename))

            print ('Searching "%s" in "%s"' % (s3_path, bucket))
            try:
                client.head_object(Bucket=bucket, Key=s3_path)
                print ("Path found on S3! Skipping %s..." % s3_path)

                # try:
                    # client.delete_object(Bucket=bucket, Key=s3_path)
                # except:
                    # print "Unable to delete %s..." % s3_path
            except:
                print ("Uploading %s..." % s3_path)
                client.upload_file(local_path, bucket, s3_path)
        stage2_df_postgres(point_df_final,poly_df_final)
        print('completed')
    else:
        print("DataFrame is Empty")

    bucket = 'aidash-rsms-main'
    destination = f'ncdot/final_data/{tool}/shapefiles/archive/' + str(dt) + '/'

    client = boto3.client('s3')

    # enumerate local files recursively
    tg_path = str(dir_path)+str(tool)+'/working/'
    for root, dirs, files in os.walk(tg_path):

      for filename in files:

        # construct the full local path
        local_path = os.path.join(root, filename)
        print(local_path)

        # construct the full Dropbox path
        relative_path = os.path.relpath(local_path, tg_path)
        s3_path = os.path.join(destination, relative_path)

        # relative_path = os.path.relpath(os.path.join(root, filename))

        print ('Searching "%s" in "%s"' % (s3_path, bucket))
        try:
            client.head_object(Bucket=bucket, Key=s3_path)
            print ("Path found on S3! Skipping %s..." % s3_path)

            # try:
                # client.delete_object(Bucket=bucket, Key=s3_path)
            # except:
                # print "Unable to delete %s..." % s3_path
        except:
            print ("Uploading %s..." % s3_path)
            client.upload_file(local_path, bucket, s3_path)
    
    s3 = boto3.resource('s3')
    for lz_file in lz_list:
        print(lz_file)
        #s3.Object(bucket, lz_file).delete()
    
    

    os.chdir(dir_path)
    #shutil.rmtree(f'{tool}', ignore_errors=True)
    
    
    #delete_command1 = ('''Drop table ncdot."point_test1"''')
    #curr.execute(delete_command1)
    #print('point table deleted')
    #delete_command2 = ('''Drop table ncdot."poly_test1"''')
    #curr.execute(delete_command2)
    #print('poly table deleted')
    #conn.commit()
    #curr.close()
    #conn.close()