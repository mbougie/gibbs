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


# arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/'
# rootpath = 'D:/projects/ksu/v2/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = '{}{}/{}/{}.gdb/'.format(rootpath,arg_list[0],arg_list[1],arg_list[2])
    # print 'gdb path: ', gdb_path 
    return gdb_path



#######  define raster and mask  ####################
class ProcessingObject(object):

    def __init__(self, series, res, mmu, years, name, subname, pixel_type, gdb_parent, parent_seq, gdb_child, mask_seq, outraster_seq):
		self.series = series
		self.res = str(res)
		self.mmu =str(mmu)


		self.years = years
		self.name = name
		self.subname = subname
		self.parent_seq = parent_seq
		self.mask_seq = mask_seq
		self.outraster_seq = outraster_seq

		self.datarange = str(self.years[0])+'to'+str(self.years[1])
		print 'self.datarange:', self.datarange

		self.dir_tiles = 'C:/Users/Bougie/Desktop/Gibbs/tiles/'

# s9_ytc30_2008to2016_mmu5_nbl_bfc

		if self.name == 'mtr':
			self.traj = self.series+'_traj_cdl'+self.res+'_b_'+self.datarange+'_rfnd'

			self.gdb_parent = defineGDBpath(gdb_parent)
			self.raster_parent = self.traj+self.parent_seq
			self.path_parent = self.gdb_parent + self.raster_parent
			print 'self.path_parent', self.path_parent

			self.gdb_child = defineGDBpath(gdb_child)
			self.raster_mask = self.raster_parent + self.mask_seq
			self.path_mask = self.gdb_child + self.raster_mask


			self.raster_nbl = self.raster_parent + self.outraster_seq
			self.path_nbl = self.gdb_child + self.raster_nbl
			print 'self.path_nbl', self.path_nbl

			self.out_fishnet = defineGDBpath(['ancillary','vector', 'shapefiles']) + 'fishnet_mtr'
			print self.out_fishnet
			self.pixel_type = "16_BIT_UNSIGNED"
				
			

		else:
			self.gdb_parent = defineGDBpath(['s14', 'post', self.name])
			self.yxc_foundation = self.series+'_'+self.name+self.res+'_'+self.datarange+'_mmu'+self.mmu
			print 'self.yxc_foundation', self.yxc_foundation
			self.path_parent = self.gdb_parent + self.yxc_foundation
			print 'self.path_parent', self.path_parent
			self.raster_mask = self.yxc_foundation + '_msk'
			self.path_mask = self.gdb_parent + self.raster_mask
			print 'self.path_mask', self.path_mask
			self.out_fishnet = defineGDBpath(['ancillary','vector', 'shapefiles']) + 'fishnet_ytc'
			self.pixel_type = "16_BIT_UNSIGNED"


			self.raster_nbl = self.yxc_foundation + '_nbl'
			print 'self.raster_nbl:', self.raster_nbl
			self.path_nbl = self.gdb_parent + self.raster_nbl
			print 'self.path_nbl', self.path_nbl


  #   def existsDataset(self):
		# dataset = self.gdb_parent + self.raster_parent + '_nbl'
		# if arcpy.Exists(dataset):
		# 	print 'dataset already exists'
		# 	return
		# else:
		# 	print 'dataset: ', dataset
		# 	return self.raster_parent + '_nbl'



def create_fishnet():
	#delete previous fishnet feature class
	arcpy.Delete_management(nibble.out_fishnet)

    #acquire parameters for creatfisnet function
	XMin = nibble.path_parent.extent.XMin
	YMin = nibble.path_parent.extent.YMin
	XMax = nibble.path_parent.extent.XMax
	YMax = nibble.path_parent.extent.YMax

	origCord = "{} {}".format(XMin, YMin)
	YAxisCord = "{} {}".format(XMin, YMax)
	cornerCord = "{} {}".format(XMax, YMax)

	cellSizeW = "0"
	cellSizeH = "0"

	numRows = 7
	numCols = 7

	geotype = "POLYGON"

	arcpy.env.outputCoordinateSystem = nibble.path_parent.spatialReference
	print nibble.path_parent.spatialReference.name

    #call CreateFishnet_management function
	arcpy.CreateFishnet_management(nibble.out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

    
  

def execute_task(args):
	in_extentDict, nibble = args


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



def mosiacRasters(nibble):
	tilelist = glob.glob(nibble.dir_tiles+'*.tif')
	print tilelist 
	######mosiac tiles together into a new raster


	arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_parent, nibble.raster_nbl, Raster(nibble.path_parent).spatialReference, nibble.pixel_type, nibble.res, "1", "LAST","FIRST")

	##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(nibble.path_nbl, "Overwrite")

	## Overwrite pyramids
	gen.buildPyramids(nibble.path_nbl)



  
def run(series, res, mmu, years, name, subname, pixel_type, gdb_parent, parent_seq, gdb_child, mask_seq, outraster_seq):  
	#instantiate the class inside run() function
	nibble = ProcessingObject(series, res, mmu, years, name, subname, pixel_type, gdb_parent, parent_seq, gdb_child, mask_seq, outraster_seq)
	print nibble.res

	# need to create a unique fishnet for each dataset
	#create_fishnet()

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
    
	# print 'extDict', extDict
	# print'extDict.items()',  extDict.items()

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	# pool = Pool(processes=1)
	pool.map(execute_task, [(ed, nibble) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(nibble)