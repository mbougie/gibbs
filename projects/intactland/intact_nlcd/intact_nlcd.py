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
# arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 


try:
    conn = psycopg2.connect("dbname='intactland' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




def reclassifyRaster(gdb_in, inraster, gdb_out, outraster, reclasslist):
	# Set environment settings
	arcpy.env.workspace = gdb_in

	#Execute Reclassify
	raster_rc = Reclassify(inraster, "Value", RemapRange(reclasslist), "NODATA")

	# Set environment settings
	arcpy.env.workspace = gdb_out

	##save raster
	raster_rc.save(outraster)

	#create pyraminds
	gen.buildPyramids(outraster)




###### call functions  #################################

###---- combine NCLD 1992 through 2011 ----might need to reclass the nlcd to 1 for 81 and 82 for each raster and then create a combine dataet


###---- export nlcd_intact attribute table to postgres -------
# gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intactland\\intact\\nlcd_intact\\nlcd_intact.gdb', pgdb='intactland', schema='nlcd_intact', table='nlcd_combine')


###---- combine nlcd_combine_binaries with binaries dataset -------


###---- export nlcd_combine_binaries attribute table to postgres -------
# gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intact_land\\intact\\nlcd_intact\\nlcd_intact.gdb', pgdb='intactland', schema='nlcd_intact', table='nlcd_combine_binary')



###---- run zonal histogram by county to create nlcd_combine_binary_counties -------


###---- export nlcd_combine_binary_counties attribute table to postgres -----
# gen.addGDBTable2postgres_histo_county(pgdb='intactland', schema='nlcd_intact', currentobject='D:\\projects\\intact_land\\intact\\nlcd_intact\\nlcd_intact.gdb\\nlcd_combine_binary_counties_t4')


# gen.addGDBTable2postgres_histo_county(pgdb='intactland', schema='intactland', currentobject='D:\\projects\\intact_land\intact\\final\intactlands.gdb\\intactlands_union_pad_raster_rc_cdl30_2015_table')


###---- label all columns with 82 or 81 in for all years (use sum )   <-- might be easier to do if working with binary datasets

###---- create new subset dataset from the postgres lookup table!!

###---- apply all filters from the intactlands to the nlcd_intact dataset to compare "apples to apples"



####add the 4 raster attribute tables from nlcd

# arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb'
gdb_nlcd = 'E:\\data\\nlcd\\nlcd.gdb'
pgdb ='intactland'

gdb_nlcd_b = 'E:\\data\\nlcd\\nlcd_b.gdb'
gdb_nlcd_intact = 'D:\\intactland\\intact_nlcd\\intact_nlcd.gdb'
# arcpy.env.workspace = gdb_nlcd_b



####step1#### combine nlcd datasets
# nlcdlist = ['nlcd_2001_land_cover_l48_20190424_rc', 'nlcd_2004_land_cover_l48_20190424_rc', 'nlcd_2006_land_cover_l48_20190424_rc', 'nlcd_2008_land_cover_l48_20190424_rc', 'nlcd_2011_land_cover_l48_20190424_rc', 'nlcd_2013_land_cover_l48_20190424_rc', 'nlcd_2016_land_cover_l48_20190424_rc']
# print nlcdlist
# outCombine = Combine(nlcdlist)
# arcpy.env.workspace = gdb_nlcd_intact
# outCombine.save("combine_nlcd_01_to_16")


####step2#### reclass trajectories to binary
# outCon = Con("combine_nlcd_b_01to16", 1, 0, "Value = 1")
# outCon.save("nlcd_intact_01to16")


###step3##### refine the dataset
gdb_nlcd = 'D:\\intactland\\intact_nlcd\\intact_nlcd.gdb'
gdb_mask = 'D:\\intactland\\intact_refine\\masks\\merged\\merged.gdb'

####apply mask to nlcd_intact
outraster = Raster('{}\\nlcd_intact_01to16'.format(gdb_nlcd)) * Raster('{}\\mask_main'.format(gdb_mask)) * Raster('{}\\mask_footprint_15_b'.format(gdb_mask))
outraster.save('{}\\nlcd_intact_01to16_refined'.format(gdb_nlcd))





# gen.addGDBTable2postgres_histo_state(gdb='D:\intactland\intact_compare\intact_compare.gdb', pgdb='intactland', schema='intact_compare', table='intact_compare_hist_state')
# gen.addGDBTable2postgres_histo_county(gdb='D:\intactland\intact_compare\intact_compare.gdb', pgdb='intactland', schema='intact_compare', table='intact_compare_hist_county')