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



#####  call main function  ###########################################################################
if __name__ == '__main__':

    usxp_rasters = ['s35_mtr', 's35_yfc', 's35_ytc', 's35_fc']
    # usxp_rasters = ['s35_fc']

    for inraster in usxp_rasters:

        print ('-----running zonal histogram-------')
        arcpy.env.workspace = 'D:\\projects\\usxp\\series\\s35\\s35_control.gdb'
        ZonalHistogram (in_zone_data='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\states', zone_field='atlas_st', in_value_raster=inraster, out_table='D:\\projects\\usxp\\series\\s35\\maps\\sm\\sm.gdb\\{}_hist'.format(inraster))

        print ('-----gen.addGDBTable2postgres_histo_stat-------')
        arcpy.env.workspace = 'D:\\projects\\usxp\\series\\s35\\maps\\sm\\sm.gdb'
        gen.addGDBTable2postgres_histo_state('usxp', 'sm', '{}_hist'.format(inraster))

