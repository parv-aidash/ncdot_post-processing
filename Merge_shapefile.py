import pandas as pd
import geopandas as gpd
import os, sys, datetime
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
# from dba import client
# import constants_v1
import pytz

out_path = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\LabelBox\Batch3LB2Poly\batch3_2.csv'
input_path = r'C:\Users\ManishaAggarwal\OneDrive - AiDash Inc\Documents\LabelBox\Batch3LB2'
# shpfiles = []
csvfiles = []
# for dirpath, subdirs, files in os.walk(input_path):
#     for x in files:
#         if x.endswith("_poly.shp"):
#             shpfiles.append(os.path.join(dirpath, x))
# #
# # concatenate shapefile dataframe
# gdf = pd.concat([
#     gpd.read_file(shp)
#     for shp in shpfiles
# ]).pipe(gpd.GeoDataFrame)

# for dirpath, subdirs, files in os.walk(input_path):
#     for y in files:
#         if y.endswith(".csv"):
#             csvfiles.append(os.path.join(dirpath, y))

#  to debug the geometry type and its shapefile location
# for shp in shpfiles:
#     try:
#         gdf = gpd.read_file(shp)
#         print(gdf.geom_type.unique(), shp)
#     except:
#         print("fail")

# gdf.to_file(out_path)
# print("success")
for dirpath, subdirs, files in os.walk(input_path):
    for y in files:
        if y.endswith(".csv"):
            csvfiles.append(os.path.join(dirpath, y))

        # concatenate csv dataframe
cdf = pd.concat([pd.read_csv(f) for f in csvfiles])
# df = pd.concat(map(pd.read_csv, files), ignore_index=True)
# cdf.rename(columns={'uuid': 'UID'}, inplace=True)
cdf.to_csv(out_path)
print("success")
