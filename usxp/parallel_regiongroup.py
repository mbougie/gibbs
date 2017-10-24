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

class ProcessingObject(object):

    def __init__(self, res, mmu, years, subtype):
        self.res = res
        self.mmu = mmu
        self.years =years 
        self.subtype = subtype


        self.datarange = str(self.years[0])+'to'+str(self.years[1])
    	self.gdb_path = defineGDBpath(['core', 'mmu'])
        self.raster = 'traj_cdl'+str(self.res)+'_b_'+self.datarange+'_rfnd_n8h_mtr'
        self.raster_path = defineGDBpath(['core', 'mtr']) + self.raster
        self.out_fishnet = defineGDBpath(['ancillary', 'temp']) + 'fishnet_' + self.subtype
        self.pixel_type = "32_BIT_UNSIGNED"
        self.dir_tiles = 'C:/Users/Bougie/Desktop/Gibbs/tiles/'



# def create_fishnet():
# 	#delete previous fishnet feature class
# 	arcpy.Delete_management(prg.out_fishnet)

#     #acquire parameters for creatfisnet function
# 	XMin = prg.raster_path.extent.XMin
# 	YMin = prg.raster_path.extent.YMin
# 	XMax = prg.raster_path.extent.XMax
# 	YMax = prg.raster_path.extent.YMax

# 	origCord = "{} {}".format(XMin, YMin)
# 	YAxisCord = "{} {}".format(XMin, YMax)
# 	cornerCord = "{} {}".format(XMax, YMax)

# 	cellSizeW = "0"
# 	cellSizeH = "0"

# 	numRows = 7
# 	numCols = 7

# 	geotype = "POLYGON"

# 	arcpy.env.outputCoordinateSystem = prg.raster_path.spatialReference
# 	print prg.raster_path.spatialReference.name

#     #call CreateFishnet_management function
# 	arcpy.CreateFishnet_management(prg.out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

  

class ModuleObject(object):
 	
 	def __init__(self, processing_object):
 		self.prg = processing_object

 	def execute_task(self, in_extentDict):
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
		arcpy.env.snapRaster = self.prg.raster_path
		arcpy.env.cellsize = self.prg.raster_path
		arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

		###  Execute Nibble  #####################

		filter_combos = {'8w':["EIGHT", "WITHIN"]}
		for k, v in filter_combos.iteritems():
			print k,v
			# Execute RegionGroup
			ras_out = RegionGroup(Raster(self.prg.raster_path), v[0], v[1],"NO_LINK")

			#clear out the extent for next time
			arcpy.ClearEnvironment("extent")

			# print fc_count
			outname = "tile_" + str(fc_count) +'.tif'

			#create Directory

			outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

			ras_out.save(outpath)
	  

def execute_task(args):
	in_extentDict, prg = args

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
	arcpy.env.snapRaster = prg.raster_path
	arcpy.env.cellsize = prg.raster_path
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Nibble  #####################

	filter_combos = {'8w':["EIGHT", "WITHIN"]}
	for k, v in filter_combos.iteritems():
	    print k,v
	    # Execute RegionGroup
	    ras_out = RegionGroup(Raster(prg.raster_path), v[0], v[1],"NO_LINK")

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
	nbl_raster = prg.raster + '_8w_msk5'
	print 'nbl_raster: ', nbl_raster

	arcpy.MosaicToNewRaster_management(tilelist, prg.gdb_path, nbl_raster, Raster(prg.raster_path).spatialReference, prg.pixel_type, prg.res, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(prg.gdb_path + nbl_raster, "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(prg.gdb_path + nbl_raster)



# def run():
# 	print 'ffffff',  prg.raster


# #### Define conversion object ######
# prg = ProcessingObject(
# 	  #resolution
# 	  '30',
# 	  #mmu
# 	  '5',
# 	  #data-range
# 	  [2008,2016],
# 	  #subtype
# 	  'mtr'
#       )

# prg = None

# if __name__ == '__main__':
def run(res, mmu, years, subtype):  
	print 'hello'

	# prg_config = ProcessingObject(**config)
	prg = ProcessingObject(res, mmu, years, subtype)
	# mod = ModuleObject(prg_config)



	print 'years', prg.years
	# need to create a unique fishnet for each dataset
	#create_fishnet()

	#remove a files in tiles directory
	tiles = glob.glob(prg.dir_tiles+"*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(prg.out_fishnet, ["SHAPE@"]):
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

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, [(ed, prg) for ed in extDict.items()])
	pool.close()
	pool.join

	createMMUmaskTiles()

	mosiacRasters()
    