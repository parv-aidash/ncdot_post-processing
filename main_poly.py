import os
import re
import argparse
import json
import uuid
import boto3
import cv2
import pandas as pd
import geopandas as gpd
import numpy as np
import warnings
warnings.simplefilter('ignore')

from shapely.geometry import Point, Polygon
from tqdm import tqdm

s3_client = boto3.client('s3')
from utils import get_bearing, get_altitude, get_lat_long, get_new_point_geopy, get_continuous_images

from distance_estimate import get_distance_from_pixel, get_pixel_size

FNAME_REGEX = re.compile(r'pano_(\d\d\d\d)_(\d\d\d\d\d\d).jpg')
s3_bucket = 'aidash-rsms-input'


def main(json_path):
    local_json_path = os.path.basename(json_path)
    tokens = json_path.split("/")
    json_bucket = tokens[2]
    json_key = "/".join(tokens[3:])

    s3_client.download_file(json_bucket, json_key, local_json_path)


    data = json.load(open(local_json_path))
    print(f"found: {len(data)} data rows in json: {local_json_path}")

    POLYS = []
    for item in tqdm(data):
        _path = item['Labeled Data']
        # 53 chars are https://aidash-rsms-input.s3.us-west-2.amazonaws.com/
        s3_path = _path[53:]
        _filename = FNAME_REGEX.search(_path)
        if not _filename:
            continue
        _filename = _filename.group()

        bboxes = item['Label']['objects']

        tokens = _filename.split("_")

        prev = False
        next_file = "pano_{}_{:>06}.jpg".format(tokens[1], int(tokens[-1][:-4]) + 1)
        prev_file = "pano_{}_{:>06}.jpg".format(tokens[1], int(tokens[-1][:-4]) - 1)

        try:
            s3_client.download_file(s3_bucket, s3_path, _filename)
        except:
            pass
        try:
            s3_client.download_file(s3_bucket, s3_path.replace(_filename, next_file), next_file)
        except:
            pass
        try:
            s3_client.download_file(s3_bucket, s3_path.replace(_filename, prev_file), prev_file)
        except:
            pass

        img = cv2.imread(f'{_filename}')
        for _bbox in bboxes:
            _crack_name = _bbox['value']
            if _crack_name in ['road_sign', 'road_signs', 'light_post', 'null', 'traffic_signals', 'variable_message_signs', 'roadway_light_poles', 'mile_marker']:
                continue
            if 'bbox' in _bbox:
                try:
                    _attrs = _bbox['classifications']
                    _merge_id = 0
                    _crack_status = None
                    for _attr in _attrs:
                        if _attr['value'] == 'merge_id':
                            _merge_id = _attr['answer']['value']
                        elif _attr['value'] == 'status':
                            _crack_status = _attr['answer']['value']

                    _box = _bbox['bbox']
                except:
                    continue

                start_point = (_box['left'], _box['top'])
                end_point = (_box['left'] + _box['width'], _box['top'] + _box['height'])

                lat, long = get_lat_long(f'{_filename}')
                if os.path.exists(next_file):
                    lat1, long1 = get_lat_long(f'{next_file}')
                    bearing = get_bearing(lat, long, lat1, long1)

                else:
                    lat1, long1 = get_lat_long(f'{prev_file}')
                    bearing = get_bearing(lat, long, lat1, long1)
                    bearing = bearing + 180

                alt = get_altitude(f'{_filename}')

                # Labelling is done top to bottom
                d_top_center = get_distance_from_pixel(start_point[1])
                d_bot_center = get_distance_from_pixel(end_point[1])

                ## Lat Long of center line of the image
                lat_top, lon_top = get_new_point_geopy(d_top_center * 0.001, bearing, lat, long)
                lat_bot, lon_bot = get_new_point_geopy(d_bot_center * 0.001, bearing, lat, long)

                pixel_size = get_pixel_size(img.shape[1] - (_box['top'] + _box['height']))
                dleft = ((img.shape[0] // 2) - start_point[0]) * pixel_size  # in meters
                dright = (end_point[0] - (img.shape[0] // 2)) * pixel_size  # in meters

                if dleft > 0:
                    lat_top_left, lon_top_left = get_new_point_geopy(dleft * 0.001, bearing - 90, lat_top, lon_top)
                    lat_bot_left, lon_bot_left = get_new_point_geopy(dleft * 0.001, bearing - 90, lat_bot, lon_bot)
                else:
                    lat_top_left, lon_top_left = get_new_point_geopy(-0.001 * dleft, bearing + 90, lat_top, lon_top)
                    lat_bot_left, lon_bot_left = get_new_point_geopy(-0.001 * dleft, bearing + 90, lat_bot, lon_bot)
                ptopleft = Point(lon_top_left, lat_top_left)
                pbotleft = Point(lon_bot_left, lat_bot_left)

                if dright > 0:
                    lat_top_right, lon_top_right = get_new_point_geopy(dright * 0.001, bearing + 90, lat_top, lon_top)
                    lat_bot_right, lon_bot_right = get_new_point_geopy(dright * 0.001, bearing + 90, lat_bot, lon_bot)
                else:
                    lat_top_right, lon_top_right = get_new_point_geopy(-0.001 * dright, bearing - 90, lat_top, lon_top)
                    lat_bot_right, lon_bot_right = get_new_point_geopy(-0.001 * dright, bearing - 90, lat_bot, lon_bot)
                ptopright = Point(lon_top_right, lat_top_right)
                pbotright = Point(lon_bot_right, lat_bot_right)

                points = [ptopleft, ptopright, pbotright, pbotleft, ptopleft]
                poly = Polygon([[p.x, p.y] for p in points])

                POLYS.append({'Name': _crack_name, 'Status': _crack_status,
                               'images': s3_path.replace('us/NC/GlobalRaymacSurveys/', ''),
                               'bbox': [_box['top'], _box['left'], _box['width'], _box['height']],
                               'prefix': tokens[0],
                               'drive_id': tokens[1],
                               'file_seq_id': tokens[2][:-4],
                               'merge_id': _merge_id,
                               'altitude': float(alt),
                                'file_id': _filename[:-4],
                               'geometry': poly})
        try:
            os.system('rm *.jpg')
        except:
            continue

    if POLYS:
        gdf = gpd.GeoDataFrame(POLYS, geometry='geometry')
        gdf.set_crs('epsg:4326', inplace=True)

        print(f"Found: {len(POLYS)} polygons({gdf.shape}) for {len(data)}")
        fname = f"{os.path.basename(json_path[:-5])}_poly"
        if not os.path.exists(f'output/{fname}'): os.makedirs(f'output/{fname}')
        gdf2 = gdf.copy()
        gdf2['bbox'] = gdf.apply(lambda row: json.dumps({'bbox': row['bbox']}), axis=1)
        gdf2.to_file(f'output/{fname}/{fname}_temp.shp')

        merge_bboxes(gdf, fname, local_json_path)


def process_gdf_csv(gdf_csv, outfile):
    image_list = []

    for i, row in gdf_csv.iterrows():
        uuid = row['uuid']
        images = json.loads(row['images'])
        boxes = json.loads(row['bboxes'])
        img_box = zip(images['src'], boxes['bbox'])
        if isinstance(images['src'], list):
            for image, bbox in img_box:
                image_list.append({
                    'uuid': uuid,
                    'images': json.dumps({'src': image}),
                    'bboxes': json.dumps({'bbox': bbox})
                })
        else:
            image_list.append({
                'uuid': uuid,
                'images': json.dumps({'src': images['src']}),
                'bboxes': json.dumps({'bbox': boxes['bbox']})
            })

    gdf_out_csv = pd.DataFrame(image_list)
    #function call for database
    gdf_out_csv.to_csv(outfile)


def merge_bboxes(gdf, fname, local_json_path):
    """Merge boxes based on mergeid"""
    ggdf = gdf.groupby(['Name', 'Status', 'merge_id', 'drive_id'])
    try:
        import shapely
        sdf = []
        df0s = []
        for name, group in ggdf:
            file_ids = group['file_seq_id'].tolist()
            crack, status, merge_id, drive_id = name
            if merge_id == 0:
                sdf_0 = group[['Name', 'Status', 'merge_id', 'geometry']]
                sdf_0['images'] = group.apply(lambda row: json.dumps({'src': row['images']}), axis = 1)
                sdf_0['bboxes'] = group.apply(lambda row: json.dumps({'bbox': row['bbox']}), axis = 1)
                df0s.append(sdf_0)
                continue
            for cont_id in get_continuous_images(file_ids):
                cont_file_ids = [f"pano_{drive_id}_{str(idx).zfill(6)}" for idx in cont_id]
                subdf = group.loc[group['file_id'].isin(cont_file_ids)]
                images = subdf.images.tolist()
                bboxes = subdf.bbox.tolist()

                union = subdf.unary_union

                _box = {'Name': crack, 'Status': status, 'merge_id': merge_id,
                        'images': json.dumps({'src': images}),
                        'bboxes': json.dumps({'bbox': bboxes}),
                        'geometry': union}

                sdf.append(_box)

        gdf_out = gpd.GeoDataFrame(sdf, geometry='geometry')
        df0s.append(gdf_out)
        gdf_out = pd.concat(df0s)
        gdf_out.set_crs('epsg:4326', inplace=True)
        gdf_out['uuid'] = [uuid.uuid4().hex for _ in range(len(gdf_out.index))]

        if not os.path.exists(f'output/{fname}'): os.makedirs(f'output/{fname}')
        gdf_out_shp = gdf_out[['uuid', 'Name', 'Status', 'merge_id', 'geometry']]
        gdf_out_shp.to_file(f'output/{fname}/{fname}.shp')
        gdf_out_csv = gdf_out[['uuid', 'images', 'bboxes']]
        csv_out = f'output/{fname}/{fname}.csv'
        print(f"CSV generated at: {csv_out}")
        process_gdf_csv(gdf_out_csv, csv_out)

        s3_out_bucket = 'aidash-rsms-main'
        s3_out_path = f"ncdot/final_data/labelbox/shapefiles/delivery_2"
        print(f'Uploading the extracted views to s3://{s3_out_bucket}/{s3_out_path}')
        s3_client.upload_file(f"output/{fname}/{fname}.csv", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}.csv")

        #main shapefile
        s3_client.upload_file(f"output/{fname}/{fname}.shp", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}.shp")
        s3_client.upload_file(f"output/{fname}/{fname}.shx", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}.shx")
        s3_client.upload_file(f"output/{fname}/{fname}.prj", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}.prj")
        s3_client.upload_file(f"output/{fname}/{fname}.cpg", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}.cpg")
        s3_client.upload_file(f"output/{fname}/{fname}.dbf", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}.dbf")

        # temp shapefile
        s3_client.upload_file(f"output/{fname}/{fname}.shp", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}_temp.shp")
        s3_client.upload_file(f"output/{fname}/{fname}.shx", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}_temp.shx")
        s3_client.upload_file(f"output/{fname}/{fname}.prj", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}_temp.prj")
        s3_client.upload_file(f"output/{fname}/{fname}.cpg", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}_temp.cpg")
        s3_client.upload_file(f"output/{fname}/{fname}.dbf", s3_out_bucket, f"{s3_out_path}/{fname}/{fname}_temp.dbf")

        # Json file
        s3_client.upload_file(local_json_path, s3_out_bucket, f"{s3_out_path}/{fname}/{local_json_path}")

    except Exception as err:
        print(err)
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--json')

    args = parser.parse_args()

    main(args.json)