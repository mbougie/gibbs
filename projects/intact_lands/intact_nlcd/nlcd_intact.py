# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import fnmatch


arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 


try:
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




# def reclassRaster():




###### call functions  #################################

###combine NCLD 1992 through 2011   ----might need to reclass the nlcd to 1 for 81 and 82 for each raster and then create a combine dataet


###export nlcd_intact attribute table to postgres
# gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intact_land\\intact\\nlcd_intact\\nlcd_intact.gdb', pgdb='intact_lands', schema='nlcd_intact', table='nlcd_combine')


#### combine nlcd_combine_binaries with binaries dataset


###export nlcd_combine_binaries attribute table to postgres
# gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intact_land\\intact\\nlcd_intact\\nlcd_intact.gdb', pgdb='intact_lands', schema='nlcd_intact', table='nlcd_combine_binary')



#####run zonal histogram by county to create nlcd_combine_binary_counties


###export nlcd_combine_binary_counties attribute table to postgres
# gen.addGDBTable2postgres_histo_county(pgdb='intact_lands', schema='nlcd_intact', currentobject='D:\\projects\\intact_land\\intact\\nlcd_intact\\nlcd_intact.gdb\\nlcd_combine_binary_counties_t4')


# gen.addGDBTable2postgres_histo_county(pgdb='intact_lands', schema='intact_lands', currentobject='D:\\projects\\intact_land\intact\\final\intactlands.gdb\\intactlands_union_pad_raster_rc_cdl30_2015_table')


###label all columns with 82 or 81 in for all years (use sum )   <-- might be easier to do if working with binary datasets

###create new subset dataset from the postgres lookup table!!

###apply all filters from the intactlands to the nlcd_intact dataset to compare "apples to apples"



####add the 4 raster attribute tables from nlcd

# arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb'
nlcd_list = ['nlcd30_1992','nlcd30_2001','nlcd30_2006','nlcd30_2011']
for nlcd in nlcd_list:

	gen.addGDBTable2postgres_raster(gdb='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb', pgdb='intact_lands', schema='misc', table=nlcd)