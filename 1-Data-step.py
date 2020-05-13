import spatialIUU.processGFW as siuu
import os
import pandas as pd
import shapefile
from shapely.geometry import shape, Point
from pyproj import Proj, transform

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
beg_date = '2017-09-01'
end_date = '2017-10-01'

# San Diego 
siuu.compileData('2017-09-01', '2017-10-01', 1, parallel=True, ncores=20)


# --------------------------------------------
# Remove all points on land
def check(lon, lat):
    # build a shapely point from your geopoint
    plon, plat = transform(Proj(init='epsg:4326'), Proj(init='epsg:3857'), lon, lat)
    point = Point(plon, plat)

    # the contains function does exactly what you want
    return polygon.contains(point)


aisdat = pd.read_feather("data/SanDiego_2017-09-01_2017-10-01.feather")

# read your shapefile
r = shapefile.Reader("data/California_shapefile/CA_State_TIGER2016.shp")

# get the shapes
shapes = r.shapes()

# build a shapely polygon from your shape
polygon = shape(shapes[0])    

check(round(-118.077119, 1), round(33.725751, 1))

#aisdat2 = aisdat[aisdat.timestamp == "2017-09-01 01:00:00"]

print("remove vessels on land")
aisdat.loc[:, 'in_CA'] = aisdat.apply(lambda x: check(x['vessel_A_lon'], x['vessel_A_lat']), axis = 1)
aisdat_save = aisdat[aisdat.in_CA == False]

aisdat_onland = aisdat[aisdat.in_CA == True]

aisdat_save.to_feather("data/SanDiego_2017-09-01_2017-10-01.feather")


aisdat_onland.to_feather("data/SanDiego_ONLAND_2017-09-01_2017-10-01.feather")