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
import numpy as np, sys, os
import pandas as pd
import collections
from collections import namedtuple
import psycopg2
from sqlalchemy import create_engine
import gdal



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = '{}{}/{}/{}.gdb/'.format(rootpath,arg_list[0],arg_list[1],arg_list[2])
    print 'gdb path: ', gdb_path 
    return gdb_path



gdal.AllRegister()

# open the image
inDs = gdal.Open("C:/Users/Bougie/Desktop/Gibbs/temp/test_temp.tif")


# create the output image
# driver = inDs.GetDriver()
driver = gdal.GetDriverByName('GTiff')



#######  define raster and mask  ####################
class ProcessingObject(object):

    def __init__(self, series, res, mmu, years, name):
		self.series = series
		self.res = str(res)
		self.mmu = mmu
		self.years = years
		self.name = name
		self.datarange = str(self.years[0])+'to'+str(self.years[1])
		self.gdb_path = defineGDBpath(['refine','v2','ytc'])

		self.inYTC_name = 'ytc'+self.res+'_'+self.datarange
		self.inYTC = defineGDBpath(['refine','v2','ytc'])+self.inYTC_name

		self.inTraj_name = 'v2_traj_'+ self.inYTC_name
		self.inTraj = defineGDBpath(['refine','v2', 'trajectories'])+self.inTraj_name

		self.inComp = defineGDBpath(['ancillary','raster','temp'])+'composite'

		self.out_fishnet = defineGDBpath(['ancillary','vector','shapefiles']) + 'fishnet_' + self.name
		self.pixel_type = "8_BIT_UNSIGNED"
		self.dir_tiles = 'C:/Users/Bougie/Desktop/Gibbs/tiles/'
		self.rows = self.createReclassifyList()
		print self.rows




    def createReclassifyList(self):
		cur = conn.cursor()

		query = (
		"SELECT DISTINCT \"Value\" "
		"FROM refinement."+self.inTraj_name+" "
		"WHERE 61 = traj_array[2] "
		"OR '{37,36}' = traj_array "
		"OR '{152,36}' = traj_array "
		"OR '{176,36}' = traj_array"
		)

		print query

		cur.execute(query)
		#create empty list
		fulllist=[[0,0,"NODATA"]]

		# fetch all rows from table
		rows = cur.fetchall()
		return rows
		print 'number of records in lookup table', len(rows)




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




def execute_task(args):
	in_extentDict, nibble = args

	fc_count = in_extentDict[0]
	print fc_count

	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	#The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = nibble.inYTC
	arcpy.env.cellsize = nibble.inYTC
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	cls = 21973
	rws = 13789


	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((13789, 21973), dtype=np.int)
	# print 'outData: ', outData


	arr_ytc = arcpy.RasterToNumPyArray(in_raster=nibble.inYTC, lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)
	arr_comp = arcpy.RasterToNumPyArray(in_raster=nibble.inComp, lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)
	arr_traj = arcpy.RasterToNumPyArray(in_raster=nibble.inTraj, lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)
	rows = [(82,), (493,), (29,), (94,), (310,), (10,), (47,), (97,), (86,), (96,), (117,), (229,), (190,), (16,), (95,), (114,), (184,), (864,), (900,), (149,), (408,)]
	# find the location of each pixel labeled with specific arbitray value in the rows list  
	for row in nibble.rows:
	    # print 'arbitrary trajectory label:', row[0]

	    #Return the indices of the elements that are non-zero.
	    thelist = (arr_traj == row[0]).nonzero()
	    # print 'thelist----', thelist

	    ww=np.column_stack((thelist[0],thelist[1]))
	    # print ww
	    # print 'len----', len(ww)
	    count = 0
	    for x in ww:
	        yearlist=range(arr_ytc[x[0],x[1]], 2017)
	        # print 'yearlist----', yearlist
	        bandindexstart = 9 - len(yearlist)
	        bandindexlist=range(bandindexstart, 9)
	        # print bandindexlist

	        for index, bandindex in enumerate(bandindexlist):
	            currentband = arr_comp[bandindex]
	            # print currentband[x[0],x[1]]
	            bandindexlist[index] = currentband[x[0],x[1]]
	        # print bandindexlist

	        if bandindexlist.count(bandindexlist[0]) == len(bandindexlist):
	            # print '-----------------same--------------------------------'
	            # print bandindexlist
	            # print 'x:',x[0]
	            # print 'y:',x[1]
	            outData[x[0],x[1]] = 1


	arcpy.ClearEnvironment("extent")

	# print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	#create
	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	outDs = driver.Create(outpath, cls, rws, 1, gdal.GDT_Int32)

	outBand = outDs.GetRasterBand(1)
		
	# # write the data
	outBand.WriteArray(outData)

	# flush data to disk, set the NoData value and calculate stats
	outBand.FlushCache()

	PIXEL_SIZE = 30  # size of the pixel...        

	outDs.SetGeoTransform((XMin,PIXEL_SIZE,0,YMax,0,-PIXEL_SIZE))  
	# outDs.SetGeoTransform(inDs.GetGeoTransform())
	outDs.SetProjection(inDs.GetProjection())




def mosiacRasters(nibble):
	######Description: mosiac tiles together into a new raster


	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print tilelist 

	#### Note: Need to set the environment for the CopyRaster_management() function or will have misallignemnt!!
	arcpy.env.workspace =  defineGDBpath(['refine','v2','masks'])
	

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inYTC=Raster(nibble.inYTC)
	inTraj=Raster(nibble.inTraj)

	arcpy.env.extent = inYTC.extent
	arcpy.env.snapRaster = inYTC
	arcpy.env.cellsize = inYTC
	arcpy.env.outputCoordinateSystem = inYTC

	# mosaic = 'traj_ytc30_2008to2015_mask'

	masks_gdb = defineGDBpath(['refine','v2','masks'])

	##sub-optimal need to create this temp dataset or and then copy are remove the dataset otherwise are not alligned
	out_name = nibble.inTraj_name+'_msk36and61_temp'

	outpath = masks_gdb+out_name


	##### CreateRasterDataset_management (out_path, out_name, cellsize=30, pixel_type, raster_spatial_reference, number_of_bands)
	# arcpy.CreateRasterDataset_management(masks_gdb, out_name, 30, "8_BIT_UNSIGNED", inTraj.spatialReference, 1, "", "", "", "", "")

	##### Mosaic_management (inputs, target, {mosaic_type}, {colormap}, {background_value}, {nodata_value}, {onebit_to_eightbit}, {mosaicking_tolerance}, {MatchingMethod})
	# arcpy.Mosaic_management(tilelist, outpath, "", "", "", 0, "", "", "")

	##### copy raster so it "snaps" to the other datasets -------suboptimal
	##### CopyRaster_management (in_raster, out_rasterdataset, {config_keyword}, {background_value}, {nodata_value}, {onebit_to_eightbit}, {colormap_to_RGB}, {pixel_type}, {scale_pixel_value}, {RGB_to_Colormap}, {format}, {transform})
	# arcpy.CopyRaster_management(Raster(outpath), nibble.inTraj_name+'_msk36and61')
	arcpy.CopyRaster_management(outpath, nibble.inTraj_name+'_msk36and61_t3', "", "", "256", "NONE", "NONE", "", "NONE", "NONE", "", "NONE")

	##### delete the initial raster
	# arcpy.Delete_management(outpath)




    





def run(series, res, mmu, years, name):  
	#instantiate the class inside run() function
	nibble = ProcessingObject(series, res, mmu, years, name)

	# tiles = glob.glob(nibble.dir_tiles+"*")
	# for tile in tiles:
	# 	os.remove(tile)

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
    
	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	# pool = Pool(processes=5)
	# # pool.map(execute_task, extDict.items())
	# pool.map(execute_task, [(ed, nibble) for ed in extDict.items()])
	# pool.close()
	# pool.join

	mosiacRasters(nibble)



    
   
