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
tool = 'labelbox'
point_path = '/Users/parv.agarwal/Desktop/merge/'
#os.system('ogr2ogr -f "ESRI Shapefile" output/output_2d.shp Cable_Rail.shp -dim 2')
line_path = '/Users/parv.agarwal/Desktop/processed/div13_GIS_10082022_batch3_LB_poly/div13_GIS_10082022_batch3_LB_poly.shp'
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
print(df1.columns)

##point_shpfiles = []
##for dirpath, subdirs, files in os.walk(path):
##    for x in files:
##        if x.endswith(".shp"):
##            point_shpfiles.append(os.path.join(dirpath, x))
#            
##ldf = pd.concat([
##    gpd.read_file(shp)
##    for shp in point_shpfiles
##]).pipe(gpd.GeoDataFrame)
##df = ldf.to_crs('EPSG:4326')
##df = gpd.read_file(path)
##print(df.MaintCnt)
##
##print(df)
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
#            
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
