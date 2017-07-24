import arcpy
import multiprocessing
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 

in_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc'
ras1 = arcpy.Raster(in_raster)



out_fishnet = defineGDBpath(['ancillary','temp'])+'fishnet_7'


def create_fishnet(in_raster, out_fishnet):
	#delete previous feature class
	arcpy.Delete_management(out_fishnet)

    #acquire parameters for creatfisnet function
	# ras1 = arcpy.Raster(in_raster)
	XMin = ras1.extent.XMin
	YMin = ras1.extent.YMin
	XMax = ras1.extent.XMax
	YMax = ras1.extent.YMax

	origCord = "{} {}".format(XMin, YMin)
	YAxisCord = "{} {}".format(XMin, YMax)
	cornerCord = "{} {}".format(XMax, YMax)

	cellSizeW = "0"
	cellSizeH = "0"

	numRows = 7
	numCols = 7

	geotype = "POLYGON"

	arcpy.env.outputCoordinateSystem = ras1.spatialReference
	print ras1.spatialReference.name

    #call CreateFishnet_management function
	arcpy.CreateFishnet_management(out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

    #create list of tiles from zonal stats that have pixel values in them
	fields = arcpy.ListFields("c:/users/bougie/desktop/gibbs/temp/zonal_stats","OID*")
	list_zonal = []
	for field in fields:
		fnf=(os.path.splitext(field.name)[0]).split("_")
		print int(fnf[1])
		list_zonal.append(int(fnf[1]))
	print list_zonal 

    #create list of all the tiles from the fishnet
	fishnet_list = []
	field = "OID"
	cursor = arcpy.SearchCursor(out_fishnet)
	for row in cursor:
	    fishnet_list.append(row.getValue(field))
	print fishnet_list

    #find the difference between the 2 lsit that represent tiles with no pixels of value
	null_tiles = set(fishnet_list).difference(list_zonal)
	print null_tiles

    #delete null tiles from fishnet feature class
	with arcpy.da.UpdateCursor(out_fishnet, "OID") as cursor:
	    for row in cursor:
	        if row[0] in null_tiles:
	            cursor.deleteRow()
    
        
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
	arcpy.env.snapRaster = in_raster
	arcpy.env.cellsize = in_raster
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)
    
    #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	# ras_out = arcpy.sa.SquareRoot(in_raster_path)
    
 #    #clear out the extent for next time
	# arcpy.ClearEnvironment("extent")

	# outname = "try22" + str(fc_count) +'.tif'

	# outpath = os.path.join(os.getcwd(), r"local_rast_wspace", outname)

	# ras_out.save(outpath)

	#########################################################################
	#define gdb workspace
	# arcpy.env.workspace=defineGDBpath(gdb_args_in)

	#declare variables but dont intialize them
	# in_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc'
	# print 'in_raster: ', in_raster

	in_mask_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_mask'
	print 'in_mask_raster: ', in_mask_raster



	###  Execute Nibble  #####################
	ras_out = arcpy.sa.Nibble(in_raster, in_mask_raster, "DATA_ONLY")
	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
    
    # print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/temp", r"tiles", outname)

	ras_out.save(outpath)


if __name__ == '__main__':

	#get fishnet
	# create_fishnet(in_raster, out_fishnet)

	# get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(out_fishnet, ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
    
	print extDict
	print extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join
    
	templist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/temp/tiles/*.tif")
	print templist 
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(glob.glob("C:/Users/Bougie/Desktop/Gibbs/temp/tiles/*.tif"), defineGDBpath(['post','yfc']), 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc_fnl', ras1.spatialReference, "8_BIT_UNSIGNED", "30", "1", "LAST","FIRST")

    ##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(defineGDBpath(['post','yfc']) + 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc_fnl', "Overwrite")
