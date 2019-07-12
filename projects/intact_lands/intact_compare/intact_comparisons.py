# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os, subprocess
import fiona
import geopandas
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



# try:
#     conn = psycopg2.connect("dbname='lem' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
# except:
#     print "I am unable to connect to the database"






# gen.addGDBTable2postgres_fc(gdb='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb', pgdb='intact_lands', schema='intact_lands', table='intactlands_union_pad')



# gen.convertFCtoPG(gdb='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb', pgdb='intact_lands', schema='intact_lands', table='intactlands_union_pad_102003_19001_multipart', epsg=102003)

def step0():
	###subset and reclass state_30
	inraster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states_30m'
	outraster='D:\\projects\\intactland\\intact_clu\\final\\intactlands_new.gdb\\states_30m_region'
	# reclasslist=[[19,0],[27,0],[30,0],[31,0],[38,0],[46,0],[56,0]]
	reclasslist=[[13,0],[21,0],[24,0],[25,0],[32,0],[39,0],[48,0]]
	reclass1 = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
	reclass1.save(outraster)










def step1():
	# gen.convertPGtoFC(gdb='D:\\projects\\intact_land\\intact\\final\\intactlands_new.gdb', pgdb='intact_lands', schema='intact_lands', table='intact_lands_per_county')
    
	# create intact_clu_b
    # Con(IsNull("intactlands_union_pad_raster"),"states_30m_region",1)


    ###export intact_compare to postgres
    # gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intactland\\intact_clu\\final\\intactlands_new.gdb', pgdb='intact_lands', schema='intact_compare', table='intact_compare')

    ##export intact_compare_cdl2015_nlcd2011 to postgres
    gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intactland\\intact_clu\\final\\intactlands_new.gdb', pgdb='intact_lands', schema='intact_compare', table='intact_compare_cdl2015_nlcd2011')



def step2():
	print('-----step2-----')
	#### get the clu polygons added per year







def step3():
	# intact_raster='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster'
	# # nlcd30_2011='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2011'
	# reclass=gen.null2value(in_raster=intact_raster, true_value=0, false_value=intact_raster)
	# reclass.save("D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster_zero")


	# gen.createReclassifyList(pgdb, query)
	# def reclassRaster(inraster, outraster, query):


	arcpy.env.workspace = 'D:\\projects\\intactland\\intact_nlcd\\nlcd_intact.gdb'

	#####  create the nlcd_intact dataset by reclassing the nlcd_combine to binaries
	raster_reclass = Reclassify(('nlcd_combine'), "Value", RemapRange(gen.createReclassifyList(pgdb='intact_lands', query='SELECT value, b FROM intact_nlcd.nlcd_combine WHERE b<>0')), "NODATA")
	print('saving the reclassed object-------------')
	raster_reclass.save('intact_nlcd')


	# create the nlcd_clu_intact_compare




def step4():

	print 'stepx-------'
	intact_raster='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster'
	cdl30_2015='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\cdl.gdb\\cdl30_2015'
	inraster="D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster_rc_cdl30_2015"

	reclass = Con(IsNull(intact_raster), intact_raster, cdl30_2015)

	# Save the output 
	reclass.save("D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster_rc_cdl30_2015")



	ZonalHistogram(inZoneData, zone_field, inraster, out_table)





########call functions#######################################
# step0()
# step1()
# step2()
step3()
# step4()










##################  OLD NOTES  ############################################################

# step3_part()###########################################################
# intact_raster='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster'
# inraster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states_30m'
# nlcd30_2011='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2011'
# outraster='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\intactlands_union_pad_raster_binary_t3'
# reclasslist=[[19,1],[27,1],[30,1],[31,1],[38,1],[46,1],[56,1]]
# reclass1 = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
#  reclass2 = Con(IsNull(intact_raster), reclass1, 0)
# reclass2.save(outraster)
# print 'finished outReclass-------------------'


# step3_part()###########################################################
# reclass2 = Con(inraster, nlcd30_2011)
# outraster='D:\\projects\\intact_land\\intact\\final\\intactlands.gdb\\nlcd30_2011_extent_t3'
# reclass2.save(outraster)





