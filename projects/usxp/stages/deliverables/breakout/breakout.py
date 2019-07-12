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

###DESCRIPTION: get the majority value per block (categorical values)



def processingCluster(instance, inraster, outraster):
	######  block stats  ###############################################

	blocksize=scale_dict[instance['scale']]
	print 'blocksize:', blocksize

	nbr = NbrRectangle(blocksize, blocksize, "CELL")
	outBlockStat = BlockStatistics(inraster, nbr, "MAJORITY", "DATA")
	print 'finished block stats.............'
	outBlockStat.save(outraster)


	addField(outraster, blocksize)


	gen.buildPyramids_new(outraster, 'NEAREST')





def addField(raster, value):
	normalizer = value*value
	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
	arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

	cur = arcpy.UpdateCursor(raster)

	for row in cur:
		row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
		cur.updateRow(row)


# D:\projects\usxp\deliverables\s26\s26.gdb
# D:\projects\usxp\deliverables\s26\maps\breakout\breakout.gdb

def main(instance):
	print('instance:', instance)

	inraster=Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_{1}'.format(instance['series'], instance['inraster_suffix']))
	print inraster


	# for scale in instance['scale'].keys():
	outraster = 'D:\\projects\\usxp\\deliverables\\{0}\\maps\\{1}\\{1}.gdb\\{0}_{2}_bs{3}'.format(instance['series'], instance['map_name'], instance['inraster_suffix'], instance['scale'])

	print outraster

	processingCluster(instance, inraster, outraster)



####global dictionary####################################
scale_dict = {'3km':100, '6km':200, '9km':300}


instance = {'series':'s35', 'inraster_suffix':'fc', 'map_name':'breakout', 'scale':'3km'}
print instance
main(instance)












