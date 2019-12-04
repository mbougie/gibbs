
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





def processingCluster(instance, inraster, outraster, reclasslist):
	#####  reclass  ####################################################
	## Reclassify (in_raster, reclass_field, remap, {missing_values})

	outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
	print 'finished outReclass-------------------'


	######  block stats  ###############################################
	##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})

	for key, value in instance['scale'].iteritems():

		outAggregate = Aggregate(in_raster=outReclass, cell_factor=value, aggregation_type="SUM", extent_handling="EXPAND", ignore_nodata="DATA")
		print 'finished Aggregate.............'
		outAggregate.save(outraster)

		# nbr = NbrRectangle(value, value, "CELL")
		# outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
		# print 'finished block stats.............'
		# outBlockStat.save(outraster)

		addField(outraster, value)

		# gen.buildPyramids_new(outraster, 'NEAREST')





def addField(raster, value):
	normalizer = value*value
	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
	arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

	cur = arcpy.UpdateCursor(raster)

	for row in cur:
		row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
		cur.updateRow(row)




def main(instance):
	inraster=Raster('I:\\d_drive\\projects\\usxp\\series\\{0}\\{0}.gdb\\{0}_bfc'.format(instance['series']))

	for key, reclasslist in instance['reclass'].iteritems():
		for scale in instance['scale'].keys():
			outraster = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\x_to_crop\\tif\\{0}_agg_{1}_sum_{2}.tif'.format(instance['series'], scale, key)

			print outraster

			processingCluster(instance, inraster, outraster, reclasslist)








# scale_dict = {'3km':100, '6km':200, '9km':300}


instance = {'series':'s35', 'scale':{'3km':100}, 'reclass':{'forest':[[63,1],[141,1],[142,1],[143,1]], 'wetland':[[83,1],[87,1],[190,1],[195,1]], 'grassland':[[37,1],[62,1],[171,1],[176,1],[181,1]], 'shrubland':[[64,1],[65,1],[131,1],[152,1]]} }

print instance

main(instance)




















