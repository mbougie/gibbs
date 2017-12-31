import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen





'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
# case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template




data = getJSONfile()
print data
  

def execute_task():
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
	arcpy.env.snapRaster = nibble.path_parent
	arcpy.env.cellsize = nibble.path_parent
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Nibble  #####################
	ras_out = arcpy.sa.Nibble(nibble.path_parent, nibble.path_mask, "DATA_ONLY")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
    
    # print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	#create Directory

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	ras_out.save(outpath)



# def mosiacRasters(nibble):
# 	tilelist = glob.glob(nibble.dir_tiles+'*.tif')
# 	print tilelist 
# 	######mosiac tiles together into a new raster


# 	arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_parent, nibble.raster_nbl, Raster(nibble.path_parent).spatialReference, nibble.pixel_type, nibble.res, "1", "LAST","FIRST")

# 	##Overwrite the existing attribute table file
# 	arcpy.BuildRasterAttributeTable_management(nibble.path_nbl, "Overwrite")

# 	## Overwrite pyramids
# 	gen.buildPyramids(nibble.path_nbl)



def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj


	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['refine']['gdb'], data['refine']['mask_nlcd']['filename'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['refine']['mask_nlcd']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_nlcd']['path'])







  
def run():  
	#remove a files in tiles directory
	tiles = glob.glob(nibble.dir_tiles+"*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(nibble.out_fishnet, ["SHAPE@"]):
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
	# pool = Pool(processes=1)
	pool.map(execute_task, [(ed, nibble) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(nibble)