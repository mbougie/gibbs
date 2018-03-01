import sys
import os
#import modules from other folders
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import arcpy
from arcpy import env
from arcpy.sa import *
import glob

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
import general as gen
import json
import fnmatch
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import multiprocessing



engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



###make this a general function
def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\current_instance.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template


##create global objects to reference through the script
data = getJSONfile()


def execute_task(in_extentDict):

	stco_atlas = "tile_"+str(in_extentDict[0])
	
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	#The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	cls = 3577
	rws = 13789

	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((rws, cls), dtype=np.int)

	ytc = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s19\\post\\ytc_s19.gdb\\s19_ytc30_2008to2017_mmu5_focal', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	megan = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\data_2008_2012.gdb\\ytc_ff2_resampled30', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj_rfnd']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)

	# find the location of each pixel labeled with specific arbitray value in the rows list  
	indices = np.where(ytc==2011)

	#stack indices so easier to work with
	stacked_indices=np.column_stack((indices[0],indices[1]))
    
    #get the x and y location of each pixel that has been selected from above
	for pixel_location in stacked_indices:
		row = pixel_location[0] 
		col = pixel_location[1]
        
		outData[row,col] = 1
		if megan[row][col] != 2011 and megan[row][col] != 0:
			outData[row,col] = traj[row][col]
  
		
	

	arcpy.ClearEnvironment("extent")

	outname = stco_atlas +'.tif'

	# #create
	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	# NumPyArrayToRaster (in_array, {lower_left_corner}, {x_cell_size}, {y_cell_size}, {value_to_nodata})
	myRaster = arcpy.NumPyArrayToRaster(outData, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=0)


	myRaster.save(outpath)



def addTrajArrayField(tablename, fields):
    #this is a sub function for addGDBTable2postgres()
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields)
    # print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE test.{} ADD COLUMN traj_array integer[];'.format(tablename));
    
    #DML: insert values into new array column
    cur.execute('UPDATE test.{} SET traj_array = ARRAY[{}];'.format(tablename, columnList));
    
    conn.commit()
    print "Records created successfully";
    # conn.close()







def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj


	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/temp.gdb', 's19_compare_megan', inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management('C:/Users/Bougie/Desktop/Gibbs/data/usxp/temp.gdb/s19_compare_megan', "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids('C:/Users/Bougie/Desktop/Gibbs/data/usxp/temp.gdb/s19_compare_megan')




    





# def run():  
if __name__ == '__main__':
    #######clear the tiles from directory
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_refine'], ["OID@","SHAPE@"]):
		oid = row[0]
		print 'oid:',oid
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[oid] = ls

    
	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=9)
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join

	mosiacRasters()
    
   
