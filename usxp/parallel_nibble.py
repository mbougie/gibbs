import arcpy
import multiprocessing
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
# import general as gen

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

working_dir = "C:/Users/Bougie/Desktop/Gibbs/temp/"

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
ras_name = 'ytc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl'
in_raster = arcpy.Raster(defineGDBpath(['post','ytc'])+ras_name)
# in_raster = arcpy.Raster(ras1)

# in_mask_raster = defineGDBpath(['sensitivity_analysis','mmu'])+'traj_cdl_b_n4h_mtr_8w_msk23'
in_mask_raster = arcpy.Raster(defineGDBpath(['post','ytc'])+ras_name+'_mask')
# in_mask_raster = arcpy.Raster(ras2)
# print 'in_mask_raster: ', in_mask_raster


out_fishnet = defineGDBpath(['post','ytc'])+'fishnet_ytc'


def create_fishnet(in_raster, out_fishnet):
	#delete previous fishnet feature class
	arcpy.Delete_management(out_fishnet)

    #acquire parameters for creatfisnet function
	XMin = in_raster.extent.XMin
	YMin = in_raster.extent.YMin
	XMax = in_raster.extent.XMax
	YMax = in_raster.extent.YMax

	origCord = "{} {}".format(XMin, YMin)
	YAxisCord = "{} {}".format(XMin, YMax)
	cornerCord = "{} {}".format(XMax, YMax)

	cellSizeW = "0"
	cellSizeH = "0"

	numRows = 7
	numCols = 7

	geotype = "POLYGON"

	arcpy.env.outputCoordinateSystem = in_raster.spatialReference
	print in_raster.spatialReference.name

    #call CreateFishnet_management function
	arcpy.CreateFishnet_management(out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

    #create list of tiles from zonal stats that have pixel values in them
	# fields = arcpy.ListFields("c:/users/bougie/desktop/gibbs/temp/zonal_stats","OID*")
	# list_zonal = []
	# for field in fields:
	# 	fnf=(os.path.splitext(field.name)[0]).split("_")
	# 	print int(fnf[1])
	# 	list_zonal.append(int(fnf[1]))
	# print list_zonal 

 #    #create list of all the tiles from the fishnet
	# fishnet_list = []
	# field = "OID"
	# cursor = arcpy.SearchCursor(out_fishnet)
	# for row in cursor:
	#     fishnet_list.append(row.getValue(field))
	# print fishnet_list

 #    #find the difference between the 2 lists that represent tiles with no pixels of value
	# null_tiles = set(fishnet_list).difference(list_zonal)
	# print null_tiles

 #    #delete null tiles from fishnet feature class
	# with arcpy.da.UpdateCursor(out_fishnet, "OID") as cursor:
	#     for row in cursor:
	#         if row[0] in null_tiles:
	#             cursor.deleteRow()
    
        
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
	arcpy.env.snapRaster = ras1
	arcpy.env.cellsize = ras1
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Nibble  #####################
	ras_out = arcpy.sa.Nibble(ras1, in_mask_raster, "DATA_ONLY")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
    
    # print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/temp", r"tiles", outname)

	ras_out.save(outpath)



# def mosiacRasters():
# 	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/temp/tiles/*.tif")
# 	print tilelist 
# 	######mosiac tiles together into a new raster
# 	arcpy.MosaicToNewRaster_management(tilelist, defineGDBpath(['sensitivity_analysis','mmu']), 'traj_cdl_b_n8h_mtr_8w_mask23_fnl', in_raster.spatialReference, "8_BIT_UNSIGNED", "30", "1", "LAST","FIRST")

# 	##Overwrite the existing attribute table file
# 	arcpy.BuildRasterAttributeTable_management(defineGDBpath(['sensitivity_analysis','mmu']) + 'traj_cdl_b_n8h_mtr_8w_mask23_fnl', "Overwrite")
    
#     ## Overwrite pyramids
#     gen.buildPyramids(defineGDBpath(['sensitivity_analysis','mmu']) + 'traj_cdl_b_n8h_mtr_8w_mask23_fnl')







if __name__ == '__main__':

	# need to create a unique fishnet for each dataset
	create_fishnet(in_raster, out_fishnet)

	# # get extents of individual features and add it to a dictionary
	# extDict = {}
	# count = 1 

	# for row in arcpy.da.SearchCursor(out_fishnet, ["SHAPE@"]):
	# 	extent_curr = row[0].extent
	# 	ls = []
	# 	ls.append(extent_curr.XMin)
	# 	ls.append(extent_curr.YMin)
	# 	ls.append(extent_curr.XMax)
	# 	ls.append(extent_curr.YMax)
	# 	extDict[count] = ls
	# 	count+=1
    
	# print extDict
	# print extDict.items()

	# #######create a process and pass dictionary of extent to execute task
	# pool = Pool(processes=cpu_count())
	# pool.map(execute_task, extDict.items())
	# pool.close()
	# pool.join

	# mosiacRasters()
    
