
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
# import general_deliverables as gen_dev


arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def processingCluster(instance, inraster, outraster, reclasslist):
	#####  reclass  ####################################################
	## Reclassify (in_raster, reclass_field, remap, {missing_values})

	outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
	print 'finished outReclass-------------------'


	######  block stats  ###############################################
	##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})

	for key, value in instance['scale'].iteritems():

		nbr = NbrRectangle(value, value, "CELL")
		outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
		print 'finished block stats.............'
		outBlockStat.save(outraster)

		addField(outraster, value)

		gen.buildPyramids_new(outraster, 'NEAREST')





def addField(raster, value):
	normalizer = value*value
	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
	arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

	cur = arcpy.UpdateCursor(raster)

	for row in cur:
		row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
		cur.updateRow(row)




def reclassRaster(inraster, reclasslist, outraster):
	outReclass = Reclassify(Raster(inraster), "Value", RemapValue(reclasslist), "NODATA")
	print 'saving outReclass object to raster'
	outReclass.save(outraster)
	print 'finished outReclass-------------------'


def blockStats(inraster, outraster, blockStatistics_args):
	nbr = NbrRectangle(blockStatistics_args['scale'], blockStatistics_args['scale'], "CELL")
	outBlockStat = BlockStatistics(Raster(inraster), nbr, blockStatistics_args['statistics_type'], "DATA")
	
	outFloat = Float(outBlockStat)
	outFloat.save(outraster)

	gen.buildPyramids_new(outraster, 'NEAREST')



def aggregateData(inraster, outraster, cell_factor):
	###Execute Aggregate
	outAggreg = Aggregate(inraster, cell_factor=cell_factor, aggregation_type='SUM', extent_handling='EXPAND', ignore_nodata='DATA')

	# Save the output 
	outAggreg.save(outraster)






def setNull(inraster, outraster, setNull_args):
	outSetNull = SetNull(in_conditional_raster=Raster(inraster), in_false_raster_or_constant=setNull_args['in_false_raster_or_constant'], where_clause=setNull_args['where_clause'])
	outSetNull.save(outraster)




def getPerc(inraster1, inraster2, outraster):
	outDivide = Divide(in_raster_or_constant2=Raster(inraster1), in_raster_or_constant1=Raster(inraster2))
	outDivide.save(outraster)


def numpyGetPerc(inraster1, inraster2, outraster):


	#set environments
	arcpy.env.snapRaster = inraster1
	arcpy.env.cellsize = inraster1
	arcpy.env.outputCoordinateSystem = inraster1
	XMin = arcpy.GetRasterProperties_management(inraster1, "LEFT")
	print(XMin)	
	XMin = -2356095
	YMin = arcpy.GetRasterProperties_management(inraster1, "BOTTOM")
	print(YMin)
	YMin = 276915
	nrows = arcpy.GetRasterProperties_management(inraster1, "ROWCOUNT")
	print(nrows)
	nrows = 96523
	ncols = arcpy.GetRasterProperties_management(inraster1, "COLUMNCOUNT")
	print(ncols)
	ncols = 153811
	# XMin = extent[0]
	# YMin = extent[1]
	arr_traj = arcpy.RasterToNumPyArray(in_raster=Raster(inraster2), lower_left_corner = arcpy.Point(XMin,YMin), nrows = nrows, ncols = ncols)




def main(instance):

	## reclassify cld2008 to noncrop and then apply a block stat ###################################
	# reclassRaster(inraster=instance['cdl30_2008_noncrop']['inraster'], reclasslist=instance['cdl30_2008_noncrop']['reclasslist'], outraster=instance['cdl30_2008_noncrop']['outraster'])
	###### 3km
	# blockStats(inraster=instance['cdl30_2008_noncrop_bs3km_sum']['inraster'], outraster=instance['cdl30_2008_noncrop_bs3km_sum']['outraster'], blockStatistics_args=instance['cdl30_2008_noncrop_bs3km_sum']['blockStatistics_args'])
	# aggregateData(inraster='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop', outraster='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop_agg3km_sum')
	###### 9km
	aggregateData(inraster='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop', outraster='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop_agg9km_sum', cell_factor=300)


	## reclassify s35_mtr to 1 and then apply a block stat  ########################################
	# setNull(inraster=instance['s35_mtr3_rc']['inraster'], outraster=instance['s35_mtr3_rc']['outraster'], setNull_args=instance['s35_mtr3_rc']['setNull_args'])
	# blockStats(inraster=instance['s35_mtr3_rc_bs3km_sum']['inraster'], outraster=instance['s35_mtr3_rc_bs3km_sum']['outraster'], blockStatistics_args=instance['s35_mtr3_rc_bs3km_sum']['blockStatistics_args'])
	
	###### 9km
	aggregateData(inraster='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_mtr3_rc', outraster='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_mtr3_rc_agg9km_sum', cell_factor=300)

	##determine the difference between the two block stats datasets to get loss of noncrop per block
	# numpyGetPerc(inraster1=instance['s35_loss']['inraster1'], inraster2=instance['s35_loss']['inraster2'], outraster=instance['s35_loss']['outraster'])
######  define the instance  #####################################
instance = {
			'cdl30_2008_noncrop': {
								'inraster':'I:\\e_drive\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2008',
							 	'outraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cld30_2008_noncrop',
								'reclasslist':[[63,1],[141,1],[142,1],[143,1],[83,1],[87,1],[190,1],[195,1],[37,1],[62,1],[171,1],[176,1],[181,1],[64,1],[65,1],[131,1],[152,1]] 
								},
			'cdl30_2008_noncrop_bs3km_sum': {
								'inraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop',
							 	'outraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop_bs3km_sum',
							 	'blockStatistics_args':{'scale':100, 'statistics_type':'SUM'}
								},


			's35_mtr3_rc': {
							'inraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\s35.gdb\\s35_mtr',
							'outraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_mtr3_rc',
							'setNull_args':{'in_false_raster_or_constant':1, 'where_clause':'Value <> 3'}
							},
			's35_mtr3_rc_bs3km_sum': {
									'inraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_mtr3_rc',
									'outraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_mtr3_rc_bs3km_sum',
									'blockStatistics_args':{'scale':100, 'statistics_type':'SUM'}
									},
			's35_loss': {
						'inraster1':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\cdl30_2008_noncrop_bs3km_sum',
						'inraster2':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_mtr3_rc_bs3km_sum',
						'outraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb\\s35_percloss'
						}


			}




######  call main function  #################################
main(instance)