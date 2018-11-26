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




def createReclassifyList(data, yxc):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = " SELECT \"Value\", {} from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE {} IS NOT NULL".format(yxc, data['pre']['traj']['filename'],  data['pre']['traj']['lookup_name'], yxc)
	# print 'query:', query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[]
	    value=row['Value'] 
	    mtr=row[yxc]  
	    templist.append(int(value))
	    templist.append(int(mtr))
	    fulllist.append(templist)
	print 'fulllist: ', fulllist
	return fulllist


  
# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, data, yxc, subtype, traj_list = args
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

	## the finished mtr product datset
	path_mtr = Raster(data['core']['path'])

	##dataset to create tha mask 61
	# path_yfc_fnc = Raster(data['post']['yfc']['fnc_61_path'])
	path_yfc_fnc = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\post\\yfc_s35.gdb\\s35_yfc_fnc_61_w_mask_k50'
	# mask_61 = SetNull(path_yfc_fnc, path_yfc_fnc, "VALUE <> 61")
	# path_mask61 = Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\post\\yfc_s35.gdb\\s35_yfc30_2008to2017_mmu5_fnc_61_mask61')

	##dataset that will be used to fill the mask layer
	conus_nc_2010 = Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_noncrop_2010')
	
	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)
	# arcpy.env.mask = mask_61
	# arcpy.env.mask = path_mask61

	##  Execute the three functions  #####################

	####fill in the null values ####################################
	filled_1 = FocalStatistics(conus_nc_2010,NbrRectangle(3, 3, "CELL"),'MAJORITY')
	conus_nc_2010=None
	filled_2 = FocalStatistics(filled_1,NbrRectangle(5, 5, "CELL"),'MAJORITY')
	filled_1=None
	filled_3 = FocalStatistics(filled_2,NbrRectangle(10, 10, "CELL"),'MAJORITY')
	filled_2=None
	filled_4 = FocalStatistics(filled_3,NbrRectangle(20, 20, "CELL"),'MAJORITY')
	filled_3=None
	filled_5 = FocalStatistics(filled_4,NbrRectangle(25, 25, "CELL"),'MINIMUM')
	filled_4=None
	final = SetNull(path_yfc_fnc, filled_5, "VALUE <> 61")
	filled_5 = None



	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	final.save(outpath)

	outpath=None
	final=None








def mosiacRasters(data, yxc, subtype):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	# tilelist_complete=['C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\post\\yfc_s35.gdb\\s35_yfc30_2008to2017_mmu5_fnc_61'] + tilelist
	# print tilelist_complete

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	mask_61_filename = 'fnc_61_mask'
	mask_61_path = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\post\\yfc_s35.gdb'+'\\'+mask_61_filename
	# path_yfc_fnc_61 = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\post\\yfc_s35.gdb\\s35_yfc30_2008to2017_mmu5_fnc_61'

	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['post'][yxc]['gdb'], mask_61_filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(mask_61_path, "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(mask_61_path)





def run(data, yxc, subtype):
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data, yxc)
	
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
	pool = Pool(processes=6)
	pool.map(execute_task, [(ed, data, yxc, subtype, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(data, yxc, subtype)



if __name__ == '__main__':
	run(data, yxc, subtype)