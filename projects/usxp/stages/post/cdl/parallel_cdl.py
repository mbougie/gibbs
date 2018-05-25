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
	query = " SELECT \"Value\", {} from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL".format(yxc, data['pre']['traj']['filename'],  data['pre']['traj']['lookup_name'])
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

	path_mtr = Raster(data['core']['path'])

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	##  Execute the three functions  #####################
	raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(traj_list), "NODATA")

	raster_mask = Con((path_mtr == yxc_dict[yxc]) & (raster_yxc >= 2008), raster_yxc)

	raster_yxc = None

	###reference the dictionary in current instance
	for year, cdlpath in data['post'][yxc][subtype]['cdlpaths'].iteritems():

		print year, cdlpath

		# allow raster to be overwritten
		arcpy.env.overwriteOutput = True
		print "overwrite on? ", arcpy.env.overwriteOutput

		#establish the condition
		cond = "Value = " + year
		print 'cond: ', cond

		raster_mask = Con(raster_mask, cdlpath, raster_mask, cond)

	print fc_count

	filled_1 = Con(IsNull(raster_mask),FocalStatistics(raster_mask,NbrRectangle(3, 3, "CELL"),'MAJORITY'), raster_mask)
	raster_mask=None
	filled_2 = Con(IsNull(filled_1),FocalStatistics(filled_1,NbrRectangle(10, 10, "CELL"),'MAJORITY'), filled_1)
	filled_1=None
	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	filled_2.save(outpath)
	filled_2=None
        



def mosiacRasters(data, subtype):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['post']['ytc'][subtype]['filename']
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['post']['ytc']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['post']['ytc'][subtype]['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['post']['ytc'][subtype]['path'])





def run(data, yxc, subtype):
# if __name__ == '__main__':
	####  remove a files in tiles directory

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data, yxc)

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
	pool = Pool(processes=9)
	pool.map(execute_task, [(ed, data, yxc, subtype, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data, subtype)



if __name__ == '__main__':
	run(data, yxc, subtype)