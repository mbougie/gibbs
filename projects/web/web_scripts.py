###small test to see if I can push

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
import subprocess
import time
import logging

# import gdal2tiles
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def execute_task(args):
	in_extentDict, data = args

	# for count in masks_list:
	cond = "Count <> 3"


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	in_raster = Raster(data['raster'])
	### set null the regions that are less than the mmu treshold
	raster_mask =  Con(IsNull(in_raster),0, in_raster)
	# raster_mask = SetNull(in_raster, in_raster, "VALUE = 3")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")

	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("D:\\projects\\web\\tiles\\", r"mtr", outname)

	# raster_shrink.save(outpath)
	raster_mask.save(outpath)
	raster_mask=None






def run(data_dict):
	# #get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_cdl_7_7', ["oid","SHAPE@"]):
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

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=10)
	pool.map(execute_task, [(ed, data_dict) for ed in extDict.items()])
	pool.close()
	pool.join




if __name__ == '__main__':
	# data_dict = {'raster':'D:\\projects\\web\\s35_mtr.tif'}
	# run(data_dict)






	# gdal2tiles.generate_tiles('D:\\projects\\web\\s35_mtr.tif', "D:\\projects\\web\\tiles\\mtr\\", np_processes=2, zoom='0-5')


	# merge_command = ["python", "gdal2tilesp.py", "D:\\projects\\web\\s35_mtr_init.tif", "D:\\projects\\web\\tiles"]

    os.system('gdal2tilesp.py D:\\projects\\web\\s35_mtr_init.tif D:\\projects\\web\\tiles')
	# # subprocess.call("gdal2tiles.py -z '0-12' -of D:\\projects\\web\\s35_mtr_init.tif D:\\projects\\web\\tiles")
	# subprocess.call(merge_command)