from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import geopandas as gpd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
import subprocess

extDict = {}

for row in arcpy.da.SearchCursor('D:\\projects\\intact_land\\intact\\refine\\mask\\misc.gdb\\fishnet_try', ["oid","SHAPE@"]):
    atlas_stco = row[0]
    print atlas_stco
    extent_curr = row[1].extent
    ls = []
    ls.append(extent_curr.XMin)
    ls.append(extent_curr.YMin)
    ls.append(extent_curr.XMax)
    ls.append(extent_curr.YMax)
    extDict[atlas_stco] = ls

print 'extDict', extDict
print'extDict.items',  extDict.items()
d = extDict.items()



for in_extentDict in d:
    print in_extentDict

    fc_count = in_extentDict[0]
    print fc_count
    procExt = in_extentDict[1]
    print procExt
    XMin = procExt[0]
    YMin = procExt[1]
    XMax = procExt[2]
    YMax = procExt[3]

    # set environments
    arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

    #######  ERASE ##########################################################################################
    in_features ='D:\\projects\\intact_land\\intact\\main\\years\\2015.gdb\\clu_2015_noncrop_c'
    erase_features = 'D:\\projects\\intact_land\\intact\\refine\mask\\final.gdb\\region_merged_masks_t2'
    out_feature_class = 'D:\\projects\\intact_land\\intact\\refine\\pp_erase_tiles\\clu_2015_noncrop_c_w_masks_{}'.format(fc_count)

    arcpy.Erase_analysis(in_features=in_features, erase_features=erase_features, out_feature_class=out_feature_class)


























#===========================================================================
#============  metadata functions  =========================================
#===========================================================================

#__________create the metatable for all states
# createMetaTable()

#_________  fill out  ____________________________________
# popCounts_years()



#===========================================================================
#============  processing functions  =======================================
#===========================================================================

#__________reproject the max date sf for a given year and state and store as fc in geodatabase
# mainProjectSF()


#________  merge datasets by state and year _____________________________________________________________
# mainMergeFC()


#_________  create the merged subset feature classes  ____________________________________
# mainSubsetFC()


#_________  fill out  ____________________________________
# mergeAllyearsByState()


#_________  fill out  ____________________________________
# changeCRPvaluesMT()


#_________  fill out  ____________________________________
# mergeAllyearsAllstates()




























