import arcpy
from arcpy import env
from arcpy.sa import *
from sqlalchemy import create_engine
import pandas as pd
import multiprocessing
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import general as gen


# arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

#import extension
arcpy.CheckOutExtension("Spatial")


def execute_task(args):
	in_extentDict, nibble, yo = args

	# in_extentDict, nibble = args


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	 #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = nibble['path_parent']
	arcpy.env.cellsize = nibble['path_parent']
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Nibble  #####################
	ras_out = arcpy.sa.Nibble(nibble['path_parent'], nibble['path_mask'], "DATA_ONLY")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
    
    # print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	#create Directory

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	ras_out.save(outpath)



def mosiacRasters(nibble):
	tilelist = glob.glob(nibble.dir_tiles+'*.tif')
	print tilelist 
	######mosiac tiles together into a new raster


	arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_parent, nibble.raster_nbl, Raster(nibble.path_parent).spatialReference, nibble.pixel_type, nibble.res, "1", "LAST","FIRST")

	##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(nibble.path_nbl, "Overwrite")

	## Overwrite pyramids
	gen.buildPyramids(nibble.path_nbl)



def run(data):  
	nibble = data['paths']
	yo = data['series']

	#remove a files in tiles directory
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(nibble['out_fishnet'], ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
    

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, [(ed, nibble, yo) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(nibble)