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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import webcolors as wc
import palettable
# import matplotlib as mpl

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
# import replace_61_w_hard_crop


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 




def createReclassifyList(query):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    fulllist.append(templist)
	return fulllist





def aggregateFct(in_raster, tablename, cellsize, agg_type, bs_label):
	arcpy.env.workspace = 'D:\\projects\\usxp\\series\\s35\\maps\\{0}\\{0}.gdb'.format(tablename)
	
	out_agg = Aggregate(in_raster=in_raster, cell_factor=cellsize, aggregation_type=agg_type, extent_handling="TRUNCATE", ignore_nodata="DATA")

	output_raster = "{}_{}_{}".format(tablename, bs_label, agg_type)

	out_agg.save(output_raster)
	# gen.buildPyramids(output_raster)


	




def run(query):

	print ('------------------------------------------------this is the run function------------------------------------------------------------------')
	####add the table to postgres
	# addGDBTable2postgres_hydric(gdb='D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb', pg_db='usxp_deliverables', schema=schema, table=table, fields=['mukey']+fieldlist)

	# tiles = glob.glob("D:/projects/usxp/deliverables/maps/{}/tiles/*".format(schema))
	# for tile in tiles:
	# 	os.remove(tile)

	reclass_list = createReclassifyList(query)
	in_raster = 'G:\\data\\r2\\s35\\core\\core_s35.gdb\\s35_mtr3_4_id'

	patchsize_raster='D:\\projects\\usxp\\series\\s35\\maps\\patchsize\\patchsize.gdb\\s35_patchsize'


	# ####reclass s35_mtr3_4_id with id value to count value ---see sql in parameter of run function
	# raster_reclassed = Reclassify(Raster(in_raster), "Value", RemapRange(reclass_list), "NODATA")
	# ##create a raw unblocked raster from reclassing id to count 
	# raster_reclassed.save('D:\\projects\\usxp\\deliverables\\maps\\patchsize\\patchsize.gdb\\s35_patchsize')

	### setblockstats
	aggregateFct(in_raster=patchsize_raster, tablename='patchsize', cellsize=500, agg_type="mean", bs_label="bs15km")



if __name__ == '__main__':
	print ('this is the main function')

	run(query = 'SELECT id,count FROM eric.merged_table WHERE link = 3')





###########################################################
###note:   
