import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json



# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")

arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\s25.gdb'
inRaster = 's25_ytc'

years = range(2009,2017)
for year in years:
    print year
    


def processingCluster():
    #####  set null  ####################################################
    ##SetNull (in_conditional_raster, in_false_raster_or_constant, {where_clause})
    
    inFalseRaster = 1
    whereClause = "VALUE <> {}".format(str(year))
    # Execute SetNull
    outSetNull = SetNull(inRaster, inFalseRaster, whereClause)
    print 'finished outSetNull'




    ######  block stats  ###############################################
    ##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})
    
    nbr = NbrRectangle(100, 100, "CELL")
    outBlockStat = BlockStatistics(outSetNull, nbr, "SUM", "DATA")
    print 'finished block stats'


    ######  resample  ###############################################
    ##Resample_management (in_raster, out_raster, {cell_size}, {resampling_type})
    
    arcpy.Resample_management(outBlockStat, , "3000 3000", "NEAREST")
        









