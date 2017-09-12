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
import general as gen

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path



#######  define raster and mask  ####################

class NibbleObject:

    def __init__(self, directory, gdb, raster_name, mmu, res):
        self.gdb_path = defineGDBpath([directory, gdb])
        self.raster_name = raster_name
        self.in_raster = defineGDBpath([directory, 'mtr']) + raster_name
        self.mask_name = raster_name + '_8w_msk' + mmu
        self.in_mask_raster = self.gdb_path + self.mask_name
        self.res = res
        self.out_fishnet = defineGDBpath(['ancillary', 'temp']) + 'fishnet_mtr'



def create_fishnet():
	#delete previous fishnet feature class
	arcpy.Delete_management(nibble.out_fishnet)

    #acquire parameters for creatfisnet function
	XMin = nibble.in_raster.extent.XMin
	YMin = nibble.in_raster.extent.YMin
	XMax = nibble.in_raster.extent.XMax
	YMax = nibble.in_raster.extent.YMax

	origCord = "{} {}".format(XMin, YMin)
	YAxisCord = "{} {}".format(XMin, YMax)
	cornerCord = "{} {}".format(XMax, YMax)

	cellSizeW = "0"
	cellSizeH = "0"

	numRows = 7
	numCols = 7

	geotype = "POLYGON"

	arcpy.env.outputCoordinateSystem = nibble.in_raster.spatialReference
	print nibble.in_raster.spatialReference.name

    #call CreateFishnet_management function
	arcpy.CreateFishnet_management(nibble.out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

    
  

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
	 #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = nibble.in_raster
	arcpy.env.cellsize = nibble.in_raster
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Nibble  #####################
	ras_out = arcpy.sa.Nibble(nibble.in_raster, nibble.in_mask_raster, "DATA_ONLY")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
    
    # print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	#create Directory

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	ras_out.save(outpath)



def mosiacRasters():
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print tilelist 
	######mosiac tiles together into a new raster
	nbl_raster = nibble.mask_name + '_nbl'
	print 'nbl_raster: ', nbl_raster

	arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_path, nbl_raster, Raster(nibble.in_raster).spatialReference, "8_BIT_UNSIGNED", nibble.res, "1", "LAST","FIRST")

	##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(nibble.gdb_path + nbl_raster, "Overwrite")

	## Overwrite pyramids
	gen.buildPyramids(nibble.gdb_path + nbl_raster)






#### Define conversion object ######
nibble = NibbleObject(
	  #directory
	  'core_2008to2012',
	  #gdb
	  'mmu',
	  #mtr raster
	  'traj_cdl30_b_2008to2012_rfnd_n8h_mtr',
	  #mmu
	  '5',
	  #resolution
	  '30'
      )


if __name__ == '__main__':

	#need to create a unique fishnet for each dataset
	###create_fishnet()

	##get extents of individual features and add it to a dictionary
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
    
	# print 'extDict', extDict
	# print'extDict.items()',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join

	mosiacRasters()
    
