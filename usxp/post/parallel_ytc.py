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
import json



'''######## DEFINE THESE EACH TIME ##########'''

#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



data = getJSONfile()
print data


  

def execute_task(in_extentDict):
	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	#########  Execute Nibble  #####################
	### Nibble (in_raster, in_mask_raster, {nibble_values})
	ras_out = arcpy.sa.Nibble(data['post']['ytc']['path_mmu'], data['post']['ytc']['path_mask'], "DATA_ONLY")

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
	
	######mosiac tiles together into a new raster
	# arcpy.MosaicToNewRaster_management(tilelist, data['post']['ytc']['gdb'], data['post']['ytc']['filename_nbl'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['post']['ytc']['path_nbl'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(s15_ytc30_2008to2012_mmu5_nbl)







  
if __name__ == '__main__':

	#####  remove a files in tiles directory
	# tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	# for tile in tiles:
	# 	os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_ytc'], ["SHAPE@"]):
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
	# pool = Pool(processes=5)
	# pool = Pool(processes=cpu_count())
	# pool.map(execute_task, extDict.items())
	# pool.close()
	# pool.join

	mosiacRasters()