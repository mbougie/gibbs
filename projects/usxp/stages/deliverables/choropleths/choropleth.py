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


#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")





def convertPGtoFC(gdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update {0}.gdb PG:"dbname=usxp_deliverables user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt MULTIPOLYGON'.format(gdb, schema, table)
    print command
    os.system(command)



#####  call main function  ###########################################################################
if __name__ == '__main__':

    #####________s35_conversion_______________________________________________________

    ##step1: create zonal histogram w/ mtr raster and county shapefile

    ##step2: import formatted s35_mtr_counties into postgres
    # gen.addGDBTable2postgres_histo_county('usxp_deliverables', 'choropleths', 'D:\\projects\\usxp\\series\\s35\\maps\\choropleths\\choropleths.gdb\\s35_mtr_counties')

    ##step3: run choropleths.s35_perc_conv_county.sql query to get the percent conversion per county


    ##step4: export table from postgres to featureclass in gdb
    convertPGtoFC('D:\\projects\\usxp\\series\\s35\\maps\\choropleths\\choropleths', 'choropleths', 's35_perc_conv_county')



    ######_______NRI__________________________________________________________________




















