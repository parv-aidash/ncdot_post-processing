import pandas as pd
import geopandas as gpd
import os, sys, datetime
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
from dba import client
import constants_v1
import pytz
import shutil
import pci_calculation as pci
from shapely import wkb
tool = 'labelbox'
point_path = '/Users/parv.agarwal/Desktop/merge/'
#os.system('ogr2ogr -f "ESRI Shapefile" output/output_2d.shp Cable_Rail.shp -dim 2')
#line_path = '/Users/parv.agarwal/Desktop/horus_with_images_buncombe/batch/1/Bridge_Joint_421e34e62218c87c7515521c5decce92/Bridge_Joint.shp'
line_path = '/Users/parv.agarwal/Desktop/shapefiles/division4_GIS_27102022_batch1_Horus_Point.shp'
path = line_path
#dir_path = '/Users/parv.agarwal/Desktop/shapefiles/'

#point_target_shp_name = 'yancey_GIS_29092022_batch2_LB_point.shp'
#point_target_shp_file_folder = 'yancey_GIS_29092022_batch2_LB_point'
#poly_target_shp_name = 'yancey_GIS_29092022_batch2_LB_poly.shp'
#poly_target_shp_file_folder = 'yancey_GIS_29092022_batch2_LB_poly'
#poly_out_path_temp = dir_path+tool+'/target/'+poly_target_shp_file_folder+'/temp/'+poly_target_shp_name
#point_out_path = dir_path+tool+'/target/'+point_target_shp_file_folder+'/'+point_target_shp_name
#poly_out_path = dir_path+tool+'/target/'+poly_target_shp_file_folder+'/'+poly_target_shp_name
#print(dir_path + tool +'/target/abc.csv')
#print(poly_out_path+'/' + poly_target_shp_file_folder +'.csv')
#shutil.move((dir_path + tool +'/target/abc.csv'), (dir_path+tool+'/target/'+poly_target_shp_file_folder+'/' + poly_target_shp_file_folder +'.csv'))
#path2 = '/Users/parv.agarwal/Desktop/shapefiles/labelbox/target/county_DS_20221007_missionBatch_LB_poly/temp/county_DS_20221007_missionBatch_LB_poly.shp'

df1 = gpd.read_file(path)
print(df1.columns)#
print(df1)

#point_shpfiles = []
#for dirpath, subdirs, files in os.walk(path):
#    for x in files:
#        if x.endswith(".shp"):
#            point_shpfiles.append(os.path.join(dirpath, x))
##            
#ldf = pd.concat([
#    gpd.read_file(shp)
#    for shp in point_shpfiles
#]).pipe(gpd.GeoDataFrame)
#df = ldf.to_crs('EPSG:4326')
#df = gpd.read_file(path)
def df_postgres():
    input_path = '/Users/parv.agarwal/Desktop/horus_with_images_buncombe/batch/1/'
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

#line_df , point_df = df_postgres()
print('done')

#for shp in shpfiles:
#    try:
#        df = gpd.read_file(shp)
#        if (df.geom_type.unique() == 'LineString'):
#            print('yay')
#        elif (df.geom_type.unique()== 'Point'):
#            print('yippie')
#        df.geometry = df.geometry.transform(_drop_z)
#        print(df.geom_type.unique())
#    except:
#        print("fail")

#_drop_z = lambda geom: wkb.loads(wkb.dumps(geom, output_dimension=2))
#print('first')
#print(df.geometry)
#df.geometry = df.geometry.transform(_drop_z)
##pci.main(path)
#print(df.columns)
#print('second')
#print(df.geometry)





#tool = 'labelbox'
#dir_path = '/Users/parv.agarwal/Desktop/shapefiles/'
##path = '/Users/parv.agarwal/Desktop/shapefiles/labelbox/working'
#path = dir_path + tool +'/working'
#tpath = dir_path + tool +'/target'
#poly_shpfiles = []
#csvfiles = []
#for dirpath, subdirs, files in os.walk(path):
#    for x in files:
#        if x.endswith("_poly.shp"):
#            lst = os.path.join(dirpath, x)
#            # print(lst)
#            try:
#                fdf = gpd.read_file(lst)
#                fdf["fpath"] = lst.replace('\\', '/').split("/")[-2]
#                poly_shpfiles.append(fdf)
#            except:
#                print("error")
            
#for dirpath, subdirs, files in os.walk(path):
#    for y in files:
#        if y.endswith(".csv"):
#            csvfiles.append(os.path.join(dirpath, y))
#            
## concatenate csv dataframe
#cdf = pd.concat([pd.read_csv(f) for f in csvfiles])
#local_working_dir_file = (path+'/abc.csv')
#cdf.to_csv(local_working_dir_file, sep=',')
#shutil.move(path+'/abc.csv', tpath+'/abcd.csv')
#df.to_file('/Users/parv.agarwal/Desktop/merge/county_DS_20221010_missionBatch_Horus_point.shp')
#df2 = gpd.read_file(path2)
#print(df2)
# output = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\SampleData\40lms_Sample_data\40lms_shapfiles'
# gdf.to_file(output, 'compiled.shp')
