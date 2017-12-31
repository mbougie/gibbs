import arcpy
from arcpy import env
from arcpy.sa import *
import os
import glob
import sys
import time
import logging
import multiprocessing
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import json
import psycopg2



'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
# case=['Bougie','Gibbs']

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
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



data = getJSONfile()
print data




#######  define raster and mask  ####################


	  
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
	# arcpy.env.snapRaster = Raster(raster_in)
	# arcpy.env.cellsize = Raster(raster_in)
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Region Group  #####################

	filter_combos = {'8w':["EIGHT", "WITHIN"]}
	
	for k, v in filter_combos.iteritems():
	    print k,v
	    # Execute RegionGroup
	    ras_out = RegionGroup(Raster(data['core']['path']['mtr']), v[0], v[1],"NO_LINK")
 
		#clear out the extent for next time
        arcpy.ClearEnvironment("extent")
	    
	    # print fc_count
        outname = "tile_" + str(fc_count) +'.tif'

		#create Directory
        outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

        ras_out.save(outpath)




def createMMUmaskTiles():
    root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
    rasterlist = glob.glob(root_in+"*.tif")
    print rasterlist

    for raster in rasterlist:
        print raster

        output = raster.replace('.', '_mask.')
        print output

        # for count in masks_list:
        cond = "Count < " + str(gen.getPixelCount(str(data['global']['res']), int(data['core']['mmu'])))
        print 'cond: ',cond

        outSetNull = SetNull(raster, 1, cond)

        # Save the output 
        outSetNull.save(output)


def mosiacRasters():
	######Description: mosiac mask tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*mask.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj


	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['core']['gdb'], data['core']['filename']['mmu'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['core']['path']['mmu'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['core']['path']['mmu'])



if __name__ == '__main__':

	#####  remove a files in tiles directory
	# tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	# for tile in tiles:
	# 	os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_mtr'], ["SHAPE@"]):
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
	# # pool = Pool(processes=cpu_count())
	# pool.map(execute_task, extDict.items())
	# pool.close()
	# pool.join

	# createMMUmaskTiles()

	mosiacRasters()
    