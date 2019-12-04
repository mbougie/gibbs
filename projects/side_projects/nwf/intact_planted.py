import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from numpy import copy
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

#import extension
arcpy.CheckOutExtension("Spatial")


# gen.addGDBTable2postgres_raster(gdb='I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb', 
# 								pgdb='usxp_deliverables', 
# 								schema='intactland_converted', 
# 								intable='intactlands_s35_converted', 
# 								outtable='intactlands_s35_converted')





# gen.addGDBTable2postgres_raster(gdb='I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb', 
# 								pgdb='usxp_deliverables', 
# 								schema='intactland_converted', 
# 								intable='intactlands_cdl2008', 
# 								outtable='intactlands_cdl2008')






# gen.addGDBTable2postgres_raster(gdb='I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb', 
# 								pgdb='usxp_deliverables', 
# 								schema='intactland_converted', 
# 								intable='intactlands_cdl2017', 
# 								outtable='intactlands_cdl2017')






# def reclassAndAggregate(in_raster, out_raster, remaplist, cell_factor):

# 	outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
# 	print 'finished reclassing raster.............'

# 	outAggregate = Aggregate(in_raster=outReclass, cell_factor=cell_factor, aggregation_type="SUM", extent_handling="EXPAND", ignore_nodata="DATA")
# 	print 'finished Aggregate.............'
# 	outAggregate.save(out_raster)



#################################################################################################################
#####call function to reclass and then aggregate raster ##########################################################
#################################################################################################################

#### create intactlands_cdl2017_agg3km_sum raster
# gen.reclassAndAggregate(
# in_raster = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb\\intactlands_cdl2017', 
# out_raster='I:\d_drive\projects\usxp\series\s35\deliverables\habitat_impacts\grassland_conversion\\tif\\intactlands_cdl2017_agg3km_sum.tif',
# remaplist=[[37,1],[176,1]],
# cell_factor=100
# )


#### create intactlands_cdl2017_agg3km_sum raster
# in_raster = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb\\intactlands_cdl2017', 
# out_raster='I:\d_drive\projects\usxp\series\s35\deliverables\habitat_impacts\grassland_conversion\\tif\\intactlands_cdl2017_agg3km_sum.tif',
# remaplist=[[37,1],[176,1]],
# cell_factor=100

# outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
# print 'finished reclassing raster.............'

# outAggregate = Aggregate(in_raster=outReclass, cell_factor=cell_factor, aggregation_type="SUM", extent_handling="EXPAND", ignore_nodata="DATA")
# print 'finished Aggregate.............'
# outAggregate.save(out_raster)


# Set the workspace environment to local file geodatabase


####### intactlands_cdl2017_grassland ###########################################################################################################
# arcpy.env.workspace = "I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb"
# rasters = arcpy.ListRasters("*", "GRID")
# for raster in rasters:
#     print(raster)

# in_raster = 'intactlands_cdl2017'
# out_raster='intactlands_cdl2017_grasslands'
# remaplist=[[37,1],[176,1]]


# outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
# outReclass.save(out_raster)


##_____________aggregate dataset__________




####### planted_grassland ######################################
# arcpy.env.workspace = "I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb"
# rasters = arcpy.ListRasters("*", "GRID")
# for raster in rasters:
#     print(raster)

# in_raster = Raster('I:\\e_drive\\data\\cdl\\cdl.gdb\\cdl30_2017')
# out_raster='planted'
# remaplist=[[37,1],[176,1]]


# outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
# outReclass.save(out_raster)




#################################################################
####### Intact grasslands 2017 ##################################
#################################################################
def createIntactGrasslands2017():
	

	#++++++++++++++++ cdl30_2017_grassland ++++++++++++++++++++++++++++++++
	def create_cdl30_2017_grassland(): 
		in_raster = Raster('I:\\e_drive\\data\\cdl\\cdl.gdb\\cdl30_2017')
		out_raster='cdl30_2017_grassland'
		remaplist=[[37,1],[176,1]]


		outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
		outReclass.save(out_raster)


	#++++++++++++++++ nlcd_intact92to16() +++++++++++++++++++++++++++++++

	### step1: need to combine all the nlcd binaries
	def combine_nlcd_b_92to16(): 
		print('this is were you create the combine raster')




	### step2: reclassefiy the combine raster to get nlcd_intact raster
	def nlcd_intact92to16(): 
		arcpy.env.workspace = "H:\\new_data_8_18_19\\e_drive\\data\\nlcd\\intact_nlcd.gdb"
		in_raster = 'combine_nlcd_b_92to16'
		out_raster='nlcd_intact_92to16'
		remaplist=[[1,1]]


		outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
		outReclass.save(out_raster)

	#++++++++++++++++ s35_mtr1 (non-crop class) +++++++++++++++++++++++++++
	def s35_mtr1(): 
		in_raster = 'I:\\d_drive\\projects\\usxp\\series\\s35\\s35.gdb\\s35_mtr'
		out_raster='s35_mtr1'
		remaplist=[[1,1]]


		outReclass = Reclassify(in_raster = in_raster, reclass_field = 'Value', remap=RemapValue(remaplist), missing_values='NODATA')
		outReclass.save(out_raster)



	#++++++++++++++++ intact_grasslands_2017 +++++++++++++++++++++++++++
	#### used the gui to combine these datasets
	in_raster = 'intact_grasslands_2017'
	out_raster = 'I:\\d_drive\\projects\\side_projects\\nwf\\tif\\intact_grasslands_2017_agg_3km_sum.tif'
	cell_factor = 100
	outAggregate = Aggregate(in_raster=in_raster, cell_factor=cell_factor, aggregation_type="SUM", extent_handling="EXPAND", ignore_nodata="DATA")
	print 'finished Aggregate.............'
	outAggregate.save(out_raster)




	#### call functions for createIntactGrasslands2017#######################################
	# create_cdl30_2017_grassland()
	# combine_nlcd_b_92to16()
	# nlcd_intact92to16()
	# s35_mtr1()







def createPlantdGrasslands2017():
	#used setnull with intact_grasslands_2017 and cdl30_2017_grassland to create dataset

	in_raster = 'planted_grasslands_2017'
	out_raster = 'I:\\d_drive\\projects\\side_projects\\nwf\\tif\\planted_grasslands_2017_agg_3km_sum.tif'
	cell_factor = 100
	outAggregate = Aggregate(in_raster=in_raster, cell_factor=cell_factor, aggregation_type="SUM", extent_handling="EXPAND", ignore_nodata="DATA")
	print 'finished Aggregate.............'
	outAggregate.save(out_raster)


















###set working environment
arcpy.env.workspace = "I:\\d_drive\\projects\\side_projects\\nwf\\intact_planted.gdb"

##### call functions ################################
if __name__ == '__main__':


	# createIntactGrasslands2017()
	createPlantdGrasslands2017()































##################################################################



#### planted_grassland_agg3km_sum raster
# in_raster = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\grassland_conversion.gdb\\combine_grasslands_intact_planted'
# out_raster='I:\d_drive\projects\usxp\series\s35\deliverables\habitat_impacts\grassland_conversion\\tif\\planted_agg3km_sum.tif'
# remaplist=[[3,1]]
# cell_factor=100


# gen.reclassAndAggregate(in_raster, out_raster, remaplist, cell_factor)



