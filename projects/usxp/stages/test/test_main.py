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


import test_side


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

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	##  Execute the three functions  #####################

	## this is the base yxc dataset derived from trajectory (speckles)
	raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(traj_list), "NODATA")
    
    ## clean the speckles so only left with yxc where the mtr regions fullfilling mmu requirement are
	raster_mask = Con((path_mtr == yxc_dict[yxc]) & (raster_yxc >= 2008), raster_yxc)
    
    ##delete object for memory
	raster_yxc = None

	refined_raster_mask = test_side.run(raster_mask, XMin, YMin, data['global']['years'], path_mtr)
    



def run(data, yxc, subtype):
	print data['post'][yxc][subtype]['cdlpaths']
# if __name__ == '__main__':
	####  remove a files in tiles directory

	# tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	# for tile in tiles:
	# 	os.remove(tile)

	traj_list = createReclassifyList(data, yxc)

	#get extents of individual features and add it to a dictionary
	extDict = {}

	# for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_yxc', ["oid","SHAPE@"]):
	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_mtr_subset', ["oid","SHAPE@"]):
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
	pool = Pool(processes=1)
	pool.map(execute_task, [(ed, data, yxc, subtype, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data, yxc, subtype)



if __name__ == '__main__':
	run(data, yxc, subtype)