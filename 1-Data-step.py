import spatialIUU.processGFW as siuu
import os
import pandas as pd
import shapefile
from shapely.geometry import shape, Point
from pyproj import Proj, transform
from joblib import Parallel, delayed
import multiprocessing
from datetime import datetime
import glob

# # Set global constants
# global GFW_DIR, GFW_OUT_DIR_CSV, GFW_OUT_DIR_FEATHER, PROC_DATA_LOC, MAX_SPEED, REGION, lon1, lon2, lat1, lat2

# siuu.GFW_DIR = '/data2/GFW_point/'
# siuu.GFW_OUT_DIR_CSV = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/csv/'
# siuu.GFW_OUT_DIR_FEATHER = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/feather/'
# siuu.PROC_DATA_LOC = '/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/'
# siuu.REGION = 'SanDiego'
# siuu.MAX_SPEED = 32

# # GFW_OUT_DIR_CSV = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/csv/'
# # GFW_OUT_DIR_FEATHER = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/feather/'
# # PROC_DATA_LOC = '/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/'

# # Check if dir exists and create
# os.makedirs(siuu.GFW_OUT_DIR_CSV, exist_ok=True) 
# os.makedirs(siuu.GFW_OUT_DIR_FEATHER, exist_ok=True) 
# os.makedirs(siuu.PROC_DATA_LOC, exist_ok=True) 

# siuu.region = 1
# siuu.lon1 = -122
# siuu.lon2 = -116
# siuu.lat1 = 30
# siuu.lat2 = 35


# GFW_OUT_DIR_CSV = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/csv/'
# GFW_OUT_DIR_FEATHER = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/feather/'
# PROC_DATA_LOC = '/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/'
# beg_date = '2017-09-01'
# end_date = '2017-10-01'

# # San Diego 
# siuu.compileData('2017-09-01', '2017-10-01', 1, parallel=True, ncores=20)

# --------------------------------------------
# Remove all points on land
def init_shape():
    # read your shapefile
    r = shapefile.Reader("data/California_shapefile/CA_State_TIGER2016.shp")
    # get the shapes
    shapes = r.shapes()
    # build a shapely polygon from your shape
    polygon = shape(shapes[0])    
    return polygon


def check_inCA(lon, lat):
    # build a shapely point from your geopoint
    plon, plat = transform(Proj(init='epsg:4326'), Proj(init='epsg:3857'), lon, lat)
    point = Point(plon, plat)
    # the contains function does exactly what you want
    return polygon.contains(point)


def process_inCA(dat):
    timestamp = dat.timestamp.iat[0]
    dat.loc[:, 'in_CA'] = dat.apply(lambda x: check_inCA(x['vessel_A_lon'], x['vessel_A_lat']), axis = 1)
    dat.to_csv(f"/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/daily_inCA/{timestamp}.csv")
    print(f"Saving: {timestamp}.csv")
    return 0

# Init shape CA file
polygon = init_shape()

aisdat = pd.read_feather("data/SanDiego_2017-09-01_2017-10-01.feather")

# aisdat = aisdat[aisdat.timestamp <= "2017-09-01 02:00:00"]

gb = aisdat.groupby('timestamp')
timest = [gb.get_group(x) for x in gb.groups]

pool = multiprocessing.Pool(50)
pool.map(process_inCA, timest)
pool.close()

# Combine processed files
files = glob.glob('data/daily_inCA/*.csv')

list_ = []
for file in files:
    df = pd.read_csv(file)
    list_.append(df)
    mdat = pd.concat(list_, sort=False)

mdat = mdat.iloc[:, 1:]

# Convert to data frame and save
mdat = mdat.reset_index(drop=True)
# mdat.to_csv('data/SanDiegoProcessed_2017-09-01_2017-10-01.csv')

# Get MMSI of vessels that have obs on land
mdat2 = mdat[mdat.in_CA == True]
unique_inCA = mdat2.vessel_A.unique()

# Remove vessels on land
mdat3 = mdat[-mdat['vessel_A'].isin(unique_inCA)]

# Save
mdat3.to_csv('data/SanDiegoProcessed_2017-09-01_2017-10-01.csv')

# aisdat.loc[:, 'in_CA'] = aisdat.apply(lambda x: check(x['vessel_A_lon'], x['vessel_A_lat']), axis = 1)

# aisdat.to_csv("data/SanDiego_2017-09-01_2017-10-01_test.csv")


