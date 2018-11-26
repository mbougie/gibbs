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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, data, yxc, subtype = args
	yxc_dict = {'ytc':3, 'yfc':4}

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	path_traj_rfnd = data['pre']['traj_rfnd']['path']
	print 'path_traj_rfnd:', path_traj_rfnd

	path_mtr = Raster(data['core']['path'])
	print path_mtr

	path_yfc = Raster(data['post']['yfc']['path'])
	print path_yfc



	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	

    ####fill in the null values ####################################
	step1_filled1 = Con(IsNull(raster_mask),FocalStatistics(raster_mask,NbrRectangle(3, 3, "CELL"),'MAJORITY'), raster_mask)
	raster_mask=None
	step1_filled2 = Con(IsNull(step1_filled1),FocalStatistics(step1_filled1,NbrRectangle(5, 5, "CELL"),'MAJORITY'), step1_filled1)
	step1_filled1=None
	step1_filled3 = Con(IsNull(step1_filled2),FocalStatistics(step1_filled2,NbrRectangle(10, 10, "CELL"),'MAJORITY'), step1_filled2)
	step1_filled2=None
	step1_filled4 = Con(IsNull(step1_filled3),FocalStatistics(step1_filled3,NbrRectangle(20, 20, "CELL"),'MAJORITY'), step1_filled3)
	step1_filled3=None
	step1_final = SetNull(path_mtr, step1_filled4, "VALUE <> {}".format(str(yxc_dict[yxc])))
	step1_filled4 = None


	outname = "tile_" + str(fc_count) +'.tif'

	# outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	rasterlist = ['C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb', step1_final]
	arcpy.MosaicToNewRaster_management(rasterlist, 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data', outname, path_traj_rfnd.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	####fill if 61 ####################################
	# step2_filled1 = Con(61,FocalStatistics(step1_final,NbrRectangle(3, 3, "CELL"),'MAJORITY'), step1_final)
	# step1_final=None
	# step2_filled2 = Con(61,FocalStatistics(step2_filled1,NbrRectangle(5, 5, "CELL"),'MAJORITY'), step2_filled1)
	# step2_filled1=None
	# step2_filled3 = Con(61,FocalStatistics(step2_filled2,NbrRectangle(10, 10, "CELL"),'MAJORITY'), step2_filled2)
	# step2_filled2=None
	# step2_filled4 = Con(61,FocalStatistics(step2_filled3,NbrRectangle(20, 20, "CELL"),'MAJORITY'), step2_filled3)
	# step2_filled3=None
	# step2_final = SetNull(path_mtr, step2_filled4, "VALUE <> {}".format(str(yxc_dict[yxc])))
	# step2_filled4 = None





	# outname = "tile_" + str(fc_count) +'.tif'

	# outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	# step2_final.save(outpath)

	# outpath=None
	# step2_final=None








def mosiacRasters(data, yxc, subtype):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['post'][yxc][subtype]['filename']
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['post'][yxc]['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['post'][yxc][subtype]['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['post'][yxc][subtype]['path'])




def run(data, yxc, subtype):

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	###NOTE: for arcgis NEED to subset tiles because empty tiles dont work.  FOr numpy processing it can deal with empty tiles!!!
	fishnet = 'fishnet_cdl_7_7_subset_yxc'

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["oid","SHAPE@"]):
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
    

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=4)
	pool.map(execute_task, [(ed, data, yxc, subtype) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(data, yxc, subtype)



if __name__ == '__main__':
	run(data, yxc, subtype)








