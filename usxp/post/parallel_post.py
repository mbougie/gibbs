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


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\current_instance.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



data = getJSONfile()
print data



def createReclassifyList():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = " SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL AND version = '{}' ".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup'], data['pre']['traj']['lookup_version'])
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



fulllist = [[0, 0, 'NODATA'], [20, 2011], [33, 2014], [37, 2013], [38, 2013], [44, 2015], [54, 2015], [61, 2015], [66, 2014], [72, 2014], [74, 2014], [77, 2014], [78, 2015], [109, 2010], [135, 2014], [138, 2015], [145, 2010], [147, 2012], [151, 2013], [158, 2010], [167, 2014], [174, 2010], [175, 2013], [185, 2010], [198, 2010], [203, 2015], [204, 2010], [206, 2013], [212, 2010], [213, 2010], [215, 2013], [217, 2015], [221, 2010], [226, 2013], [227, 2013], [236, 2015], [248, 2014], [249, 2015], [250, 2012], [271, 2013], [276, 2010], [279, 2010], [282, 2012], [283, 2011], [284, 2011], [287, 2012], [300, 2011], [304, 2011], [305, 2015], [317, 2010], [323, 2014], [325, 2013], [327, 2010], [329, 2013], [333, 2011], [344, 2012], [353, 2011], [354, 2014], [370, 2010], [371, 2014], [373, 2013], [374, 2014], [376, 2014], [382, 2011], [384, 2012], [385, 2015], [389, 2013], [392, 2010], [397, 2011], [399, 2012], [405, 2011], [406, 2015], [408, 2015], [410, 2012], [421, 2012], [423, 2011], [430, 2014], [434, 2014], [438, 2014], [439, 2014], [452, 2013], [454, 2011], [463, 2015], [465, 2013], [467, 2012], [468, 2012], [475, 2012], [479, 2011], [480, 2012], [483, 2014], [490, 2012], [495, 2015], [496, 2015], [497, 2011], [498, 2011], [503, 2011], [510, 2011], [511, 2011], [512, 2012]]




def execute_task(in_extentDict):
	yxc = {'ytc':3, 'yfc':4}


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	raster_in = data['pre']['traj_rfnd']['path']
	print 'raster_in:', raster_in

	path_mtr = Raster(data['core']['path']['mmu'])

	#set environments
	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute the three functions  #####################
	raster_yxc = Reclassify(Raster(raster_in), "Value", RemapRange(fulllist), "NODATA")

	raster_mask = Con((path_mtr == yxc['ytc']) & (raster_yxc >= 2008), raster_yxc)

	raster_mmu = Con((path_mtr == yxc['ytc']) & (IsNull(raster_mask)), yxc['ytc'], Con((path_mtr == yxc['ytc']) & (raster_mask >= 2008), raster_mask))

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")

	# print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	raster_mmu.save(outpath)




def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['core']['function']['parallel_mtr']['output'].replace(data['core']['gdb']+'\\', '')
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['core']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['core']['function']['parallel_mtr']['output'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['core']['function']['parallel_mtr']['output'])





if __name__ == '__main__':

	#####  remove a files in tiles directory
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['counties_subset'], ["SHAPE@"]):
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
	pool = Pool(processes=1)
	# pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join

	# mosiacRasters()