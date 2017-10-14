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

    def __init__(self, mmu, res, years, subtype):
        self.res = res
        self.mmu = mmu
        
        self.years = years
        self.subtype = subtype

        self.datarange = str(self.years[0])+'to'+str(self.years[1])
 
    	self.gdb_path = defineGDBpath(['core', 'mmu'])
        self.raster_name = 'traj_cdl'+self.res+'_b_'+self.datarange+'_rfnd_n8h_mtr'
        self.in_raster = defineGDBpath(['core', 'mtr']) + self.raster_name
        self.out_fishnet = defineGDBpath(['ancillary', 'temp']) + 'fishnet_' + self.subtype
        self.pixel_type = "32_BIT_UNSIGNED"



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

	filter_combos = {'8w':["EIGHT", "WITHIN"]}
	for k, v in filter_combos.iteritems():
	    print k,v
	    # Execute RegionGroup
	    ras_out = RegionGroup(Raster(nibble.in_raster), v[0], v[1],"NO_LINK")

		#clear out the extent for next time
        arcpy.ClearEnvironment("extent")
	    
	    # print fc_count
        outname = "tile_" + str(fc_count) +'.tif'

		#create Directory

        outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

        ras_out.save(outpath)



def createMMUmask():
    root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
    rasterlist = glob.glob(root_in+"*.tif")
    print tilelist 

    for raster in rasterlist:
        print raster

        output = raster.replace('.', '_mask.')
        print output

        # for count in masks_list:
        cond = "Count < " + str(gen.getPixelCount('30', 5))
        print 'cond: ',cond

        outSetNull = SetNull(raster, 1, cond)

        # Save the output 
        outSetNull.save(output)




def mosiacRasters():
	root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
	tilelist = glob.glob(root_in+"*mask.tif")
	print tilelist  
	######mosiac tiles together into a new raster
	nbl_raster = nibble.raster_name + '_8w_msk5'
	print 'nbl_raster: ', nbl_raster

	arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_path, nbl_raster, Raster(nibble.in_raster).spatialReference, nibble.pixel_type, nibble.res, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(nibble.gdb_path + nbl_raster, "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(nibble.gdb_path + nbl_raster)






#### Define conversion object ######
nibble = NibbleObject(
	  #mmu
	  '5',
	  #resolution
	  '30',
	  #data-range
	  [2008,2016],
	  #subtype
	  'mtr'
      )


if __name__ == '__main__':

	# need to create a unique fishnet for each dataset
	##create_fishnet()

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
    
	# print 'extDict', extDict
	# print'extDict.items()',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join
    
    createMMUmask()

	mosiacRasters()
    
