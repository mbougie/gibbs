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


######## processing steps #########################################

#### combined crop_15, non_crop, mask_final, pad to create a dataset of all classes to define the 7 state region
##Note: performed this in GUI

crop = 'D:\\intactland\\intact_clu\\main\\years\\2015_initial.gdb\\clu_2015_crop_c_b'
non_crop = 'D:\\intactland\\intact_clu\\final\\current.gdb\\intactland_15_refined'
masks = 'D:\\intactland\\intact_clu\\refine\\masks\\merged\\merged.gdb\\mask_main'
pad = 'D:\\intactland\\intact_clu\\final\\current.gdb\\pad_30m_b'

# outCombine = Combine([crop,non_crop, masks, pad])
# outCombine.save("D:\\intactland\\intact_clu\\final\\current.gdb\\combined")

### export dataset to postgres
### create sql to be used in R to visualize data in barchart








##### old code (remove if not needed) #############################################

# arcpy.CheckOutExtension("Spatial")
# #### establish environmental parameters
# clu_2015_noncrop_c_30m = Raster('D:\\intactland\\intact_clu\\main\\years\\2015_initial.gdb\\clu_2015_noncrop_c_30m')
# arcpy.env.snapRaster = clu_2015_noncrop_c_30m
# arcpy.env.cellsize = clu_2015_noncrop_c_30m
# arcpy.env.outputCoordinateSystem = clu_2015_noncrop_c_30m
# arcpy.env.extent = clu_2015_noncrop_c_30m.extent



# gdb_in = 'D:\\intactland\\intact_clu\\refine\\masks\\merged\\merged.gdb'
# gdb_out = 'D:\\intactland\\intact_clu\\final\\current.gdb'

# ###step1##### refine the footprint dataset by removing the lakes, roads, cities 
# arcpy.env.workspace = gdb_in
# mask_footprint_15_b_refined = Raster('mask_footprint_15_b') * Raster('mask_main')

# ###step2##### set both the mask_footprint_15_b_refined and intactland_15_refined datasets to null where value = 0 and then mosaic them to get dataset
# ###that contains both intact and non-intact. intact=15 and nonintact=1
# arcpy.env.workspace = gdb_out
# intactland_15_refined_nulled = SetNull("intactland_15_refined", "intactland_15_refined", "VALUE = 0")
# mask_footprint_15_b_refined_nulled = SetNull(Raster('mask_footprint_15_b'), mask_footprint_15_b_refined, "VALUE = 0")

# arcpy.MosaicToNewRaster_management([intactland_15_refined_nulled, mask_footprint_15_b_refined_nulled], gdb_out, 'test_it', clu_2015_noncrop_c_30m.spatialReference, "8_BIT_UNSIGNED", 30, "1", "LAST","FIRST")









