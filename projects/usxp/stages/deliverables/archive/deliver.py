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



def execute_task(args):
	in_extentDict, data = args



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

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	##  Execute the three functions  #####################
    # Set local variables
	inRaster = Raster("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s22\\post\\ytc_s22.gdb\\s22_ytc30_2008to2017_mmu5")
	nbr = NbrAnnulus(3000, 3000, "CELL")

	# Execute BlockStatistics
	outBlockStat = BlockStatistics(inRaster, nbr, "SUM", "DATA")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")

	# print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)
    
	outBlockStat.save(outpath)

	outpath = None

	outBlockStat = None

	# arcpy.Delete_management("in_memory")




def mosiacRasters(data, yxc):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['post'][yxc]['filename']
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['post'][yxc]['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['post'][yxc]['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['post'][yxc]['path'])







def run(data):

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_ytc'], ["oid","SHAPE@"]):
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
	pool = Pool(processes=2)
	pool.map(execute_task, [(ed, data) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data)



if __name__ == '__main__':
	run(data)