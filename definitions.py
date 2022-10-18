import uuid
import random

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
    df.drop_duplicates(subset=['SyncID'])  # dropping duplicate data
    return df

query_lb_poly = '''SELECT q."FeatureName", q."FeatureStatus", q."FeatRouteID", q."lat", q."lon", q."geohash", 
    q."Frame_numb", q."Ref_No", q."images",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", 
    foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", 
    foo."RouteCla", foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry FROM 
    "Stage1Line_L1" AS q CROSS JOIN LATERAL 
(SELECT * FROM "NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''

query_lb_point = '''SELECT q."FeatureName", q."FeatureStatus", q."FeatRouteID", q."lat", q."lon", q."geohash", 
    q."Frame_numb", q."Ref_No", q."images",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", 
    foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", 
    foo."RouteCla", foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry FROM 
    "Stage1Line_L1" AS q CROSS JOIN LATERAL 
(SELECT * FROM "NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''

query_hr_line = '''SELECT q."FeatureName", q."FeatureStatus", q."FeatRouteID", q."lat", q."lon", q."geohash", 
    q."Frame_numb", q."Ref_No", q."images",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", 
    foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", 
    foo."RouteCla", foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry FROM 
    "Stage1Line_L1" AS q CROSS JOIN LATERAL 
(SELECT * FROM "NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''

query_hr_point = '''SELECT q."FeatureName", q."FeatureStatus", q."FeatRouteID", q."lat", q."lon", q."geohash", 
    q."Frame_numb", q."Ref_No", q."images",
    foo."Division", foo."MPLength",foo."RouteName", foo."StreetName", foo."Length", 
    foo."Width", foo."Area", foo."UUID", foo."RouteMaint", foo."RouteID", foo."BeginMp1", foo."EndMp1", 
    foo."BeginFeatu", foo."EndFeature", foo."MaxMp1", foo."Shape_Leng", foo."MaintCnt", foo."LocCntyC", 
    foo."RouteCla", foo."RouteInv", foo."Direction", foo."TravelDir", foo."UniqueID",  q.geometry FROM 
    "Stage1Line_L1" AS q CROSS JOIN LATERAL 
(SELECT * FROM "NC_REF_DATA" AS d ORDER BY q.geometry<->d.geometry LIMIT 1) foo; '''