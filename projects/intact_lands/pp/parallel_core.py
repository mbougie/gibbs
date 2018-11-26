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
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def execute_task(args):
	# in_extentDict, data, traj_list = args
	in_extentDict = args


	fc_count = in_extentDict[0]
	print fc_count
	procExt = in_extentDict[1]
	print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)



	#######  BUFFER ##########################################################################################

	# roads_buffer = "D:\\projects\\intact_land\\intact\\refine\\tiles_t2\\roads_buffer_{}.shp".format(fc_count)

	# arcpy.Buffer_analysis("D:\\projects\\intact_land\\intact\\refine\\mask\\roads.gdb\\region_roads", roads_buffer , "25 meters", "FULL", "ROUND", "ALL")
	if fc_count == '27':

		#######  ERASE ##########################################################################################
		in_features ='D:\\projects\\intact_land\\intact\\main\\years\\2015.gdb\\clu_2015_noncrop_c'
		erase_features = 'D:\\projects\\intact_land\\intact\\refine\mask\\final.gdb\\region_merged_masks_t2'
		out_feature_class = 'D:\\projects\\intact_land\\intact\\refine\\pp_erase\\clu_2015_noncrop_c_w_masks_{}'.format(fc_count)

		arcpy.Erase_analysis(in_features=in_features, erase_features=erase_features, out_feature_class=out_feature_class)







def mosiacRasters(data):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['core']['filename']
	print 'filename:-----------------------------', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['core']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['core']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['core']['path'])







def run():

	# tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	# for tile in tiles:
	# 	os.remove(tile)

	# traj_list = createReclassifyList(data)

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('D:\\projects\\intact_land\\intact\\refine\\mask\\misc.gdb\\states_region', ["atlas_st","SHAPE@"]):
		atlas_stco = row[0]
		print atlas_stco
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[atlas_stco] = ls

	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#####create a process and pass dictionary of extent to execute task
	pool = Pool(processes=1)
	pool.map(execute_task, [(ed) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data)


if __name__ == '__main__':
	run()
