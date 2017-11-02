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
# import general as gen

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'D:/projects/ksu/v2/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


#######  define raster and mask  ####################

class ProcessingObject(object):

    def __init__(self, res, mmu, years):
        self.res = str(res)
        self.mmu = str(mmu)
        # self.years =years
        self.out_fishnet = defineGDBpath(['ancillary', 'shapefiles']) + 'fishnet_subset'
        self.clu = defineGDBpath(['main', 'merged_clu']) + 'clu2008county_1cm_buffer'
        self.yans = defineGDBpath(['main', 'yan_roy']) + 'yan_roy_aeac'




def create_fishnet():
	#delete previous fishnet feature class
	arcpy.Delete_management(prg.out_fishnet)

    #acquire parameters for creatfisnet function
	XMin = prg.raster_path.extent.XMin
	YMin = prg.raster_path.extent.YMin
	XMax = prg.raster_path.extent.XMax
	YMax = prg.raster_path.extent.YMax

	origCord = "{} {}".format(XMin, YMin)
	YAxisCord = "{} {}".format(XMin, YMax)
	cornerCord = "{} {}".format(XMax, YMax)

	cellSizeW = "0"
	cellSizeH = "0"

	numRows = 7
	numCols = 7

	geotype = "POLYGON"

	arcpy.env.outputCoordinateSystem = prg.raster_path.spatialReference
	print prg.raster_path.spatialReference.name

    #call CreateFishnet_management function
	arcpy.CreateFishnet_management(prg.out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

  
	  
def execute_task(in_extentDict):
	fc_count = in_extentDict[0]
	print fc_count
	procExt = in_extentDict[1]
	print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	 #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	# arcpy.env.snapRaster = prg.clu
	# arcpy.env.cellsize = prg.clu
	# arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Nibble  #####################


	# import arcpy
	# from arcpy import env

	print 'hi'

	# env.workspace = "C:/data"
	outname = "tile_" + str(fc_count) + ".shp"

	outpath = os.path.join("D:/projects/ksu/v2/", r"tiles", outname)

	arcpy.Clip_analysis(prg.clu, prg.out_fishnet, outpath)

	arcpy.ClearEnvironment("extent")


def clipFCtoFishnet(el):
	# self.out_fishnet = defineGDBpath(['ancillary', 'shapefiles']) + 'fishnet_subset'
	arcpy.env.workspace = defineGDBpath(['ancillary', 'shapefiles'])
	# Use the ListFeatureClasses function to return a list of shapefiles.
	fc = 'fishnet_subset'

	# cursor = arcpy.da.SearchCursor(fc, ['OBJECTID'])
	# for row in cursor:
	#     print(row[0])
	    
	layer = "layer_" + str(el)
	where_clause = "OBJECTID = " + str(el)




	# Set local variables
	in_features = defineGDBpath(['main', 'merged_clu']) + 'clu2008county_1cm_buffer'
	clip_features = arcpy.MakeFeatureLayer_management(fc,layer, where_clause)
	# out_feature_class = defineGDBpath(['main', 'yo']) + "stco_"+str(extlist[0])
	xy_tolerance = ""

	outname = "tile_" + str(el) + ".shp"

	outpath = os.path.join("D:/projects/ksu/v2/", r"tiles", outname)

	# arcpy.Clip_analysis(prg.clu, prg.out_fishnet, outpath)

	# Execute Clip
	arcpy.Clip_analysis(in_features, clip_features, outpath, xy_tolerance)






	# filter_combos = {'8w':["EIGHT", "WITHIN"]}
	# for k, v in filter_combos.iteritems():
	#     print k,v
	#     # Execute RegionGroup
	#     ras_out = RegionGroup(Raster(prg.raster_path), v[0], v[1],"NO_LINK")

	# 	#clear out the extent for next time
 #        arcpy.ClearEnvironment("extent")
	    
	#     # print fc_count
 #        outname = "tile_" + str(fc_count) +'.tif'

	# 	#create Directory

 #        outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

 #        ras_out.save(outpath)



def createMMUmaskTiles_test(prg):
    root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
    rasterlist = glob.glob(root_in+"*.tif")
    print rasterlist

    for raster in rasterlist:
        print raster

        output = raster.replace('.', '_mask.')
        print output

        in_true_raster_or_constant = defineGDBpath(['core','mtr'])+prg.series+'_traj_cdl'+prg.res+'_b_'+prg.datarange+'_rfnd_n8h_mtr'

        # for count in masks_list:
        cond = "Count < " + str(gen.getPixelCount('30', 5))
        print 'cond: ',cond

        outSetNull = SetNull(raster, in_true_raster_or_constant, cond)

        # Save the output 
        outSetNull.save(output)



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


def mosiacRasters(prg):
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


prg = ProcessingObject(
    #resolution
    30,
    #mmu
    5,
    #data-range
    [2008,2016]
)





if __name__ == '__main__':

	# need to create a unique fishnet for each dataset
	#create_fishnet()

	#remove a files in tiles directory
	# tiles = glob.glob(prg.dir_tiles+"*")
	# for tile in tiles:
	# 	os.remove(tile)

	#get extents of individual features and add it to a dictionary
	# extDict = {}
	# count = 1 

	# for row in arcpy.da.SearchCursor(prg.out_fishnet, ["SHAPE@"]):
	# 	extent_curr = row[0].extent
	# 	ls = []
	# 	ls.append(extent_curr.XMin)
	# 	ls.append(extent_curr.YMin)
	# 	ls.append(extent_curr.XMax)
	# 	ls.append(extent_curr.YMax)
	# 	extDict[count] = ls
	# 	count+=1

	# print 'extDict', extDict
	# print'extDict.items()',  extDict.items()

	extlist = [1,2,3,4]

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=5)
	pool.map(clipFCtoFishnet, extlist)
	pool.close()
	pool.join

	# createMMUmaskTiles()

	# mosiacRasters(prg)
    