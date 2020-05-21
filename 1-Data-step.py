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

# -------------------------------------------
# [1]Processing raw GFW Data

# Set global constants
global GFW_DIR, GFW_OUT_DIR_CSV, GFW_OUT_DIR_FEATHER, PROC_DATA_LOC, MAX_SPEED, REGION, lon1, lon2, lat1, lat2

siuu.GFW_DIR = '/data2/GFW_point/'
siuu.GFW_OUT_DIR_CSV = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/csv/'
siuu.GFW_OUT_DIR_FEATHER = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/feather/'
siuu.PROC_DATA_LOC = '/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/'
siuu.REGION = 'SanDiego'
siuu.MAX_SPEED = 32

# GFW_OUT_DIR_CSV = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/csv/'
# GFW_OUT_DIR_FEATHER = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/feather/'
# PROC_DATA_LOC = '/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/'

# Check if dir exists and create
os.makedirs(siuu.GFW_OUT_DIR_CSV, exist_ok=True) 
os.makedirs(siuu.GFW_OUT_DIR_FEATHER, exist_ok=True) 
os.makedirs(siuu.PROC_DATA_LOC, exist_ok=True) 

siuu.region = 1
siuu.lon1 = -122
siuu.lon2 = -116
siuu.lat1 = 30
siuu.lat2 = 35


GFW_OUT_DIR_CSV = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/csv/'
GFW_OUT_DIR_FEATHER = '/home/server/pi/homes/woodilla/Data/GFW_point/SanDiego/feather/'
PROC_DATA_LOC = '/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/'
beg_date = '2017-08-20'
end_date = '2017-10-01'

# San Diego 
siuu.compileData('2017-08-20', '2017-10-01', 1, parallel=True, ncores=20)








# -------------------------------------------
# [2] Remove all points on land

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
    dat = dat.reset_index(drop=True)
    dat.to_feather(f"/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/daily_inCA/{timestamp}.feather")
    print(f"Saving: {timestamp}.feather")
    return 0


# Separate files to daily for easier processing
def process_daily(dat):
    timestamp = dat.timestamp.iat[0]
    dat = dat.reset_index(drop=True)
    dat.to_feather(f"/home/server/pi/homes/woodilla/Projects/San-Diego-IUU/data/daily/{timestamp}.feather")
    print(f"Saving: {timestamp}.feather")
    return 0



# Init shape CA file
polygon = init_shape()

aisdat = pd.read_feather("data/SanDiego_2017-08-20_2017-10-01.feather")

# Parallel Processing
gb = aisdat.groupby('timestamp')
timest = [gb.get_group(x) for x in gb.groups]

pool = multiprocessing.Pool(50)
pool.map(process_inCA, timest)
pool.close()

# Clean environment before collecting files
del aisdat



# Combine processed files
files = glob.glob('data/daily_inCA/*.feather')

list_ = []
for file in files:
    df = pd.read_feather(file)
    list_.append(df)
    mdat = pd.concat(list_, sort=False)




mdat = mdat.sort_values('timestamp')
# mdat = mdat.dropna()

mdat_timestamp = pd.DatetimeIndex(mdat.timestamp, tz='UTC')
mdat_timestamp = mdat_timestamp.tz_convert('America/Los_Angeles')
mdat_timestamp = mdat_timestamp.strftime("%Y-%m-%d %H:%M:%S")
mdat.loc[:, 'timestamp'] = mdat_timestamp
mdat.loc[:, 'timestamp'] = pd.to_datetime(mdat.timestamp, format="%Y-%m-%d %H:%M:%S")


# Remove vessels on land
mdat2 = mdat[mdat.in_CA == False]

# Remove vessels by hand
remove_vessels = [309933000, 367786750, 319091100, 366892170, 309484000, 367485000, 338795000, 367004930, 338795000, 367729880]
mdat2 = mdat2[-mdat2['vessel_A'].isin(remove_vessels)]

# Save Processed
mdat2 = mdat2.reset_index(drop=True)
mdat2.to_feather('data/SanDiego_Processed_2017-08-20_2017-10-01.feather')

# Save Reduced columns
mdat3 = mdat2[['timestamp', 'vessel_A', 'vessel_B', 'vessel_A_lon', 'vessel_A_lat', 'distance']]

# Save data
mdat3 = mdat3.reset_index(drop=True)
mdat3.to_feather('data/SanDiego_Reduced_2017-08-20_2017-10-01.feather')


# -------------------------------------------
# [3] Separate files for parallel processing KS statistics
# mdat3 = pd.read_feather('data/SanDiegoReduced_2017-09-01_2017-10-01.feather')

# # Group by
# gb = mdat3.groupby('timestamp')
# timest = [gb.get_group(x) for x in gb.groups]

# pool = multiprocessing.Pool(50)
# pool.map(process_daily, timest)
# pool.close()