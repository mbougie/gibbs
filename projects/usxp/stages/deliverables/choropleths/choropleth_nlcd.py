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

def createCombineRaster():
    traj_reclasslist = gen.createReclassifyList(pgdb='usxp', query='SELECT value, b FROM misc.lookup_nlcd')

    nlcd30_2008 = Reclassify(Raster('nlcd30_2008'), "Value", RemapRange(traj_reclasslist), "NODATA")
    nlcd30_2016 = Reclassify(Raster('nlcd30_2016'), "Value", RemapRange(traj_reclasslist), "NODATA")


    outCombine = Combine([nlcd30_2008, nlcd30_2016])

    return outCombine

    outCombine.save("D:\\projects\\usxp\\series\\s35\\maps\\choropleths\\choropleths.gdb\\combine_nlcd08_16")



def convertPGtoFC(gdb, pgdb, query, outtable):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update {0} PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM choropleths.nlcd_08_16_perc" -nln {3} -nlt MULTIPOLYGON'.format(gdb, pgdb, query, outtable)
    print command
    os.system(command)

#####  call main function  ###########################################################################
if __name__ == '__main__':
    # Set the workspace environment to local file geodatabase
    arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb'
    # createCombineRaster()

    arcpy.env.workspace = 'D:\\projects\\usxp\\series\\s35\\maps\\choropleths\\choropleths.gdb'
    ###### create zonal stats with NWALT raster  #####################################################
    # ZonalHistogram (in_zone_data='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties', zone_field='atlas_stco', in_value_raster='combine_nlcd08_16', out_table='combine_nlcd08_16_histo')
    # gen.addGDBTable2postgres_histo_county(pgdb='usxp_deliverables', schema='choropleths', currentobject='combine_nlcd08_16_histo')


    query='''SELECT 
            combine_nlcd08_16_histo.atlas_stco, 
            counties.atlas_name, 
            counties.atlas_caps, 
            combine_nlcd08_16_histo.label, 
            combine_nlcd08_16_histo.count, 
            combine_nlcd08_16_histo.acres,
            counties.acres_calc, 
            (combine_nlcd08_16_histo.acres/counties.acres_calc)*100 as perc,
            counties.geom
            FROM 
            choropleths.combine_nlcd08_16_histo, 
            spatial.counties
            WHERE 
            counties.atlas_stco = combine_nlcd08_16_histo.atlas_stco;'''







    convertPGtoFC(gdb='D:\projects\usxp\series\s35\maps\choropleths\choropleths.gdb', pgdb='usxp_deliverables', query=query, outtable='nlcd_08_16_perc')














