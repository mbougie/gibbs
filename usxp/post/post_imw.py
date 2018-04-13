
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
	arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s21\\core\\core_s21.gdb'
	cond = 'Value <> 3'

	in_extentDict= args

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	path_mtr = Raster('s21_v5_traj_cdl30_b_2008to2010_rfnd_v4_n8h_mtr_8w_mmu5')
	print path_mtr

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	##  Execute the three functions  #####################
	raster_1 = 's21_v5_traj_cdl30_b_2008to2010_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_1 = SetNull(raster_1, '2009', cond)

	raster_2 = 's21_v5_traj_cdl30_b_2008to2011_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_2 = SetNull(raster_2, '2010', cond)

	raster_3 = 's21_v5_traj_cdl30_b_2009to2012_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_3 = SetNull(raster_3, '2011', cond)

	raster_4 = 's21_v5_traj_cdl30_b_2010to2013_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_4 = SetNull(raster_4, '2012', cond)

	raster_5 = 's21_v5_traj_cdl30_b_2011to2014_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_5 = SetNull(raster_5, '2013', cond)

	raster_6 = 's21_v5_traj_cdl30_b_2012to2015_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_6 = SetNull(raster_6, '2014', cond)

	raster_7 = 's21_v5_traj_cdl30_b_2013to2016_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_7 = SetNull(raster_7, '2015', cond)

	raster_8 = 's21_v5_traj_cdl30_b_2014to2017_rfnd_v4_n8h_mtr_8w_mmu5'
	mask_8 = SetNull(raster_8, '2016', cond)

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")

	# print fc_count
	outname = "tile_" + str(fc_count) +'.tif'


	filelist = [mask_8, mask_7, mask_6, mask_5, mask_4, mask_3, mask_2, mask_1]
	print 'filelist:', filelist


	folder = "C:/Users/Bougie/Desktop/Gibbs/tiles"
	outname = "tile_" + str(fc_count) +'.tif'

	##### mosaicRasters():
	arcpy.MosaicToNewRaster_management(filelist, folder, outname, path_mtr.spatialReference, '16_BIT_UNSIGNED', 30, "1", "LAST","FIRST")





def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
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






def run():

	#####  remove a files in tiles directory
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_ytc', ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
    
	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=5)
	# pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	# pool.map(execute_task, [(ed, data) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data, yxc)



if __name__ == '__main__':
	run()