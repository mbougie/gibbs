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

# SetNull (in_conditional_raster, in_false_raster_or_constant, {where_clause})
in_conditional_raster = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\s35.gdb\\s35_mtr'
in_false_raster = 'D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact.gdb\\mask_intact'
output = 'D:\\projects\\usxp\\deliverables\\s35\\general.gdb\\s35_mtr_nlcdIntact'
cond = "Value <> 3"

raster_out = SetNull(in_conditional_raster, in_false_raster, cond)
raster_out.save(output)