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




def createReclassifyList(data):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = " SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL".format(data['pre']['traj']['filename'], data['core']['lookup'])
	# print 'query:', query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[]
	    value=row['Value'] 
	    mtr=row['ytc']  
	    templist.append(int(value))
	    templist.append(int(mtr))
	    fulllist.append(templist)
	print 'fulllist: ', fulllist
	return fulllist


  
# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, data, year, cdlpath = args
	yxc = {'ytc':3, 'yfc':4}


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
	raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(createReclassifyList(data)), "NODATA")

	raster_mask = Con((path_mtr == yxc['ytc']) & (raster_yxc >= 2008), raster_yxc)

	# allow raster to be overwritten
	arcpy.env.overwriteOutput = True
	print "overwrite on? ", arcpy.env.overwriteOutput

	#establish the condition
	cond = "Value = " + year
	print 'cond: ', cond

	raster_mask = Con(raster_mask, cdlpath, raster_mask, cond)

	filled_1 = Con(IsNull(raster_mask),FocalStatistics(raster_mask,NbrRectangle(3, 3, "CELL"),'MAJORITY'), raster_mask)
	filled_2 = Con(IsNull(filled_1),FocalStatistics(filled_1,NbrRectangle(10, 10, "CELL"),'MAJORITY'), filled_1)
	final = SetNull(filled_2, filled_2, "VALUE > 254")


	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	final.save(outpath)
        



def mosiacRasters(data, subtype, year):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	gdb = data['post']['ytc']['gdb']

	filename = '{}_{}'.format(data['post']['ytc'][subtype]['filename'],year)
	print 'filename:', filename

	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management('{}//{}'.format(gdb,filename), "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids('{}//{}'.format(gdb,filename))





def run(data, subtype):
	for year, cdlpath in data['post']['ytc'][subtype]['cdlpaths'].iteritems():

		print year, cdlpath
		####  remove a files in tiles directory

		tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
		for tile in tiles:
			os.remove(tile)



		#get extents of individual features and add it to a dictionary
		extDict = {}

		for row in arcpy.da.SearchCursor("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_ytc", ["oid","SHAPE@"]):
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
	    

		# #######create a process and pass dictionary of extent to execute task
		pool = Pool(processes=5)
		# pool = Pool(processes=cpu_count())
		# pool.map(execute_task, extDict.items())
		pool.map(execute_task, [(ed, data, year, cdlpath) for ed in extDict.items()])
		pool.close()
		pool.join

		mosiacRasters(data, subtype, year)



if __name__ == '__main__':
	run(data, subtype)
