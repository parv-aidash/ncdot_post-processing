import os
import sys
import math
import time
from tqdm import tqdm

import pandas as pd
import numpy as np
import geopandas as gpd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# Route Shapefile
route_shapefile = "/Users/parv.agarwal/Downloads/NC_Ref_Data/NC_WGS_Final.shp"
route_df = gpd.read_file(route_shapefile)


def get_damage_part(damage, severeity, df):
    """Filter the Damage Dataframe for damage and severity"""
    sdf = df.loc[(df['FeatureNam'] == damage) & (df['FeatureSta'] == severeity)]
    _area = sum(sdf['area'].tolist())
    return _area, len(sdf)


def get_length_in_feet(rid, begin_mp, to_mp):
    """Filter respective Route and return Length (computed using QGIS) | Close to EPSG:6933"""
    _row = route_df.loc[(route_df['RouteID'] == rid) & (route_df['BeginMp1'] == begin_mp) & (route_df['EndMp1'] == to_mp)]
    _row = _row.iloc[0]

    return _row['len_ft']


def get_route_area(rid, begin_mp, to_mp):
    """ Fetched area from the Route shapefile calculated using EPSG:32633"""
    _row = route_df.loc[(route_df['RouteID'] == rid) & (route_df['BeginMp1'] == begin_mp) & (route_df['EndMp1'] == to_mp)]
    _row = _row.iloc[0]
    _area_m2 = _row.Area
    _len_m = _row.Length
    _width_m = _row.Width

    return _area_m2, _len_m, _width_m

def pci_score(route_area, length_in_ft, df):
    penalties = {}
    counts = {}
    areas = {}
    # Penalty Alligator Severe
    _area_alligator_severe, _count_severe = get_damage_part('alligator_cracking', 'severe', df)
    _area_alligator_moderate, _count_moderate = get_damage_part('alligator_cracking', 'moderate', df)
    _area_alligator_light, _count_light = get_damage_part('alligator_cracking', 'light', df)

    _area_alligator = _area_alligator_severe + _area_alligator_moderate + _area_alligator_light
    _count_alligator = _count_severe + _count_moderate + _count_light
    if _area_alligator >= route_area:
        _area_alligator_severe = (_count_severe / _count_alligator) * route_area
        _area_alligator_moderate = (_count_moderate / _count_alligator) * route_area
        _area_alligator_light = (_count_light / _count_alligator) * route_area

    _ratio_alligator_severe = int((_area_alligator_severe * 100) / route_area)
    _ratio_alligator_severe = _ratio_alligator_severe / 10.0

    if _ratio_alligator_severe <= 2:
        _penalty_severe = 15 * _ratio_alligator_severe
    else:
        _penalty_severe = 30 + ((_ratio_alligator_severe - 2) * 3)

    penalties['alligator_severe'] = _penalty_severe
    counts['alligator_severe'] = _count_severe
    areas['alligator_severe'] = _area_alligator_severe

    # Penalty Alligator Moderate
    _ratio_alligator_moderate = int((_area_alligator_moderate * 100) / route_area)
    _ratio_alligator_moderate = _ratio_alligator_moderate / 10.0
    if _penalty_severe >= 30:
        _penalty_moderate = _ratio_alligator_moderate * 2
    elif _penalty_severe > 0 and _penalty_severe < 30:
        _var = (30 - _penalty_severe) / 7.5
        if _ratio_alligator_moderate <= _var:
            _penalty_moderate = _ratio_alligator_moderate * 7.5
        else:
            _penalty_moderate = ((_ratio_alligator_moderate - _var) * 2) + (_var * 7.5)
    else:
        _penalty_moderate = 0

    penalties['alligator_moderate'] = _penalty_moderate
    counts['alligator_moderate'] = _count_moderate
    areas['alligator_moderate'] = _area_alligator_moderate

    # Penalty Alligator Light
    penalty = _penalty_moderate + _penalty_severe
    _ratio_alligator_light = int((_area_alligator_light * 100) / route_area)
    _ratio_alligator_light = _ratio_alligator_light / 10.0
    if penalty >= 30:
        _penalty_light = _ratio_alligator_light
    else:
        _var = math.ceil((30 - penalty) / 3.3)
        if _ratio_alligator_light < _var:
            _penalty_light = _ratio_alligator_light * 3.3
        else:
            _penalty_light = (_ratio_alligator_light - _var) + (_var * 3.3)

    # Total Alligator Penalty
    penalty += _penalty_light
    penalties['alligator_light'] = _penalty_light
    counts['alligator_light'] = _count_light
    areas['alligator_light'] = _area_alligator_light

    # Transverse Cracks
    _area_transverse_light, _count_transverse_light = get_damage_part('transverse_cracking', 'light', df)
    _ratio_transverse_light = int((_area_transverse_light * 100) / route_area)

    _area_transverse_moderate, _count_transverse_moderate = get_damage_part('transverse_cracking', 'moderate', df)
    _ratio_transverse_moderate = int((_area_transverse_moderate * 100) / route_area)

    _area_transverse_severe, _count_transverse_severe = get_damage_part('transverse_cracking', 'severe', df)
    _ratio_transverse_severe = int((_area_transverse_severe * 100) / route_area)

    _ratio_transverse = _ratio_transverse_light + _ratio_transverse_moderate + _ratio_transverse_severe
    max_transverse = max(_ratio_transverse_light, _ratio_transverse_moderate, _ratio_transverse_severe)
    #if _ratio_transverse >= 50:
    _rate = 40 * ((_count_transverse_light + _count_transverse_moderate + _count_transverse_severe) / (length_in_ft))
    if _rate >= 1:
        if _ratio_transverse_severe == max_transverse:
            penalty += 30
            penalties['transverse_severe'] = 30
            counts['transverse_severe'] = _count_transverse_severe
            areas['transverse_severe'] = _area_transverse_severe
        elif _ratio_transverse_moderate == max_transverse:
            penalty += 15
            penalties['transverse_moderate'] = 15
            counts['transverse_moderate'] = _count_transverse_moderate
            areas['transverse_moderate'] = _area_transverse_moderate

        elif _ratio_transverse_light == max_transverse:
            penalty += 5
            penalties['transverse_light'] = 5
            counts['transverse_light'] = _count_transverse_light
            areas['transverse_light'] = _area_transverse_light

    # Rutting Cracks
    _area_rutting_light, _count_rutting_light = get_damage_part('rutting_obvious', 'light', df)
    _ratio_rutting_light = int((_area_rutting_light * 100) / route_area)

    _area_rutting_moderate, _count_rutting_moderate = get_damage_part('rutting_obvious', 'moderate', df)
    _ratio_rutting_moderate = int((_area_rutting_moderate * 100) / route_area)

    _area_rutting_severe, _count_rutting_severe = get_damage_part('rutting_obvious', 'severe', df)
    _ratio_rutting_severe = int((_area_rutting_severe * 100) / route_area)

    _ratio_rutting = _ratio_rutting_light + _ratio_rutting_moderate + _ratio_rutting_severe
    max_rutting = max(_ratio_rutting_light, _ratio_rutting_moderate, _ratio_rutting_severe)
    if _ratio_rutting >= 50:
        if _ratio_rutting_severe == max_rutting:
            penalty += 30
            penalties['rutting_severe'] = 30
            counts['rutting_severe'] = _count_rutting_severe
            areas['rutting_severe'] = _area_rutting_severe
        elif _ratio_rutting_moderate == max_rutting:
            penalty += 20
            penalties['rutting_moderate'] = 20
            counts['rutting_moderate'] = _count_rutting_moderate
            areas['rutting_moderate'] = _area_rutting_moderate

        elif _ratio_rutting_light == max_rutting:
            penalty += 5
            penalties['rutting_light'] = 5
            counts['rutting_light'] = _count_rutting_light
            areas['rutting_light'] = _area_rutting_light

    # Ravelling Cracks
    _area_ravelling_light, _count_ravelling_light = get_damage_part('raveling', 'light', df)
    _ratio_ravelling_light = int((_area_ravelling_light * 100) / route_area)

    _area_ravelling_moderate, _count_ravelling_moderate = get_damage_part('raveling', 'moderate', df)
    _ratio_ravelling_moderate = int((_area_ravelling_moderate * 100) / route_area)

    _area_ravelling_severe, _count_ravelling_severe = get_damage_part('raveling', 'severe', df)
    _ratio_ravelling_severe = int((_area_ravelling_severe * 100) / route_area)

    _ratio_ravelling = _ratio_ravelling_light + _ratio_ravelling_moderate + _ratio_ravelling_severe
    max_ravelling = max(_ratio_ravelling_light, _ratio_ravelling_moderate, _ratio_ravelling_severe)
    if _ratio_ravelling >= 50:
        if _ratio_ravelling_severe == max_ravelling:
            penalty += 15
            penalties['ravelling_severe'] = 15
            counts['ravelling_severe'] = _count_ravelling_severe
            areas['ravelling_severe'] = _area_ravelling_severe
        elif _ratio_ravelling_moderate == max_ravelling:
            penalty += 5
            penalties['ravelling_moderate'] = 5
            counts['ravelling_moderate'] = _count_ravelling_moderate
            areas['ravelling_moderate'] = _area_ravelling_moderate

        elif _ratio_ravelling_light == max_ravelling:
            penalty += 2
            penalties['ravelling_light'] = 2
            counts['ravelling_light'] = _count_ravelling_light
            areas['ravelling_light'] = _area_ravelling_light

    # Bleeding Cracks
    _area_bleeding_light, _count_bleeding_light = get_damage_part('bleeding', 'light', df)
    _ratio_bleeding_light = int((_area_bleeding_light * 100) / route_area)

    _area_bleeding_moderate, _count_bleeding_moderate = get_damage_part('bleeding', 'moderate', df)
    _ratio_bleeding_moderate = int((_area_bleeding_moderate * 100) / route_area)

    _area_bleeding_severe, _count_bleeding_severe = get_damage_part('bleeding', 'severe', df)
    _ratio_bleeding_severe = int((_area_bleeding_severe * 100) / route_area)

    _ratio_bleeding = _ratio_bleeding_light + _ratio_bleeding_moderate + _ratio_bleeding_severe
    if _ratio_bleeding >= 50:
        penalty += 30
        penalties['bleeding_severe'] = 30
        counts['bleeding_severe'] = _count_bleeding_severe
        areas['bleeding_severe'] = _area_bleeding_severe
    elif _ratio_bleeding >= 26:
        penalty += 20
        penalties['bleeding_moderate'] = 20
        counts['bleeding_moderate'] = _count_bleeding_moderate
        areas['bleeding_moderate'] = _area_bleeding_moderate

    elif _ratio_bleeding >= 10:
        penalty += 10
        penalties['bleeding_light'] = 10
        counts['bleeding_light'] = _count_bleeding_light
        areas['bleeding_light'] = _area_bleeding_light

    # Patching Cracks
    _area_patching_light, _count_patching_light = get_damage_part('patching', 'light', df)
    _ratio_patching_light = int((_area_patching_light * 100) / route_area)

    _area_patching_moderate, _count_patching_moderate = get_damage_part('patching', 'moderate', df)
    _ratio_patching_moderate = int((_area_patching_moderate * 100) / route_area)

    _area_patching_severe, _count_patching_severe = get_damage_part('patching', 'severe', df)
    _ratio_patching_severe = int((_area_patching_severe * 100) / route_area)

    _ratio_patching = _ratio_patching_light + _ratio_patching_moderate + _ratio_patching_severe
    if _ratio_patching >= 30:
        penalty += 20
        penalties['patching_severe'] = 20
        counts['patching_severe'] = _count_patching_severe
        areas['patching_severe'] = _area_patching_severe
    elif _ratio_patching >= 15:
        penalty += 10
        penalties['patching_moderate'] = 10
        counts['patching_moderate'] = _count_patching_moderate
        areas['patching_moderate'] = _area_patching_moderate

    elif _ratio_patching >= 6:
        penalty += 5
        penalties['patching_light'] = 5
        counts['patching_light'] = _count_patching_light
        areas['patching_light'] = _area_patching_light

    # Oxidation
    _area_oxidation_severe, _count = get_damage_part('oxidation', 'severe', df)
    _ratio_oxidation_severe = int((_area_oxidation_severe * 100) / route_area)
    if _ratio_oxidation_severe > 0:
        penalty += 5
        penalties['oxidation'] = 5
        counts['oxidation'] = _count
        areas['oxidation'] = _area_oxidation_severe

    pci = 100 - penalty
    if pci < 0:
        pci = 0

    return pci, penalty, penalties, counts, areas


def main(damage_shapefile):
    """Entry Point for PCI computation"""
    import time
    tstart = time.time()
    print(f"Loading damage shapefile...")
    gdf = gpd.read_file(damage_shapefile)
    print(f"time taken: {time.time()-tstart}sec")


    print(f'Generating Segment IDs')
    gdf['seg_id'] = gdf['RouteID'].astype(str) + gdf['BeginMp1'].astype(str) + gdf['EndMp1'].astype(str)
    seg_ids = gdf.seg_id.unique().tolist()
    print(f"Found: unique {len(set(seg_ids))} / {len(seg_ids)} segments")

    print(f"Calculating area for damage polygons in EPSG:32633")
    gdf['area'] = gdf['geometry'].to_crs('epsg:32633').map(lambda p: p.area)
    print(f"time taken: {time.time()-tstart}sec")

    print(f"Starting PCI calculation...")
    import json
    import time
    PCI = []

    for _seg_id in tqdm(seg_ids):
        t1 = time.time()
        subdf = gdf.loc[gdf['seg_id'] == _seg_id]
        if subdf.empty:
            print(_seg_id)
        _row = subdf.iloc[0]
        rid = _row['RouteID']
        begin_mp = _row['BeginMp1']
        to_mp = _row['EndMp1']
        unique_id = _row['UniqueID']

        route_area_m2, length_m, width_m = get_route_area(rid, begin_mp, to_mp)
        
        pci, penalty, penalties, counts, areas = pci_score(route_area_m2, length_m*3.28084, subdf)
        if pci > 80:
            road_condition = 'good'
        elif pci > 60 and pci <= 80:
            road_condition = 'fair'
        else:
            road_condition = 'poor'
        PCI.append(
            {
                'uuid': _seg_id,
                'route_id': rid,
                'begin_mp': begin_mp,
                'to_mp': to_mp,
                'area_sqm': route_area_m2,
                'width_m': width_m,
                'len_m': length_m,
                'UniqueID': unique_id,
                'pci': pci,
                'route_condition': road_condition,
                'penalty': penalty,
                'penalties': json.dumps(penalties),
                'areas': json.dumps(areas),
                'counts': json.dumps(counts)
            })
        t2 = time.time()
        # print(f"Final PCI for Route ID: {i}: {_seg_id} = {pci} (penalty = {penalty}) (time: {round(t2 - t1, 2)}sec)")

    pci_df = pd.DataFrame(PCI)
    print(f"Generated PCI for: {len(pci_df)} segments")

    fname = os.path.basename(damage_shapefile)
    pci_file = f"pci_{fname.replace('shp', 'csv')}"
    pci_path = os.path.join(os.path.dirname(damage_shapefile), pci_file)
    print(f"Written to CSV: {pci_path}")
    pci_df.to_csv(pci_path)

    print(f"Written to CSV: {pci_path}")


if __name__ == "__main__":
    #damage_shapefile = sys.argv[1]

    #main('/Users/parv.agarwal/Desktop/shapefiles/labelbox/target/county_DS_20221007_missionBatch_LB_poly/county_DS_20221007_missionBatch_LB_poly.shp')
    print('pci calculation')