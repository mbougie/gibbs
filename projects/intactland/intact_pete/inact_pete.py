import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import fiona
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
# import general_deliverables as gen_dev

#import extension
arcpy.CheckOutExtension("Spatial")

####set environment variables #####################################################
clu_2015_noncrop_c_30m = Raster('D:\\intactland\\intact_clu\\main\\years\\2015_initial.gdb\\clu_2015_noncrop_c_30m')
arcpy.env.snapRaster = clu_2015_noncrop_c_30m
arcpy.env.cellsize = clu_2015_noncrop_c_30m
arcpy.env.outputCoordinateSystem = clu_2015_noncrop_c_30m
arcpy.env.extent = clu_2015_noncrop_c_30m.extent


env.workspace = 'D:\intactland\intact_pete\intact_pete.gdb'

# arcpy.PolygonToRaster_conversion(in_features='intact_pete', value_field='OBJECTID', out_rasterdataset='intact_pete_raster_extent', cell_assignment='CELL_CENTER', cellsize=30)

 
#####convert dataset to binary 0/1 (1 = footprint/0 = non-footprint)
outCon = Con(IsNull('intact_pete_raster'), 0, 1)
outCon.save('intact_pete_raster_b')
    

gen.buildPyramids('intact_pete_raster_b')



