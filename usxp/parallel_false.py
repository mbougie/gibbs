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
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path



#######  define raster and mask  ####################

inYTC = Raster(defineGDBpath(['refine','ytc'])+'ytc30_2008to2016')
# self.arr_ytc = arcpy.RasterToNumPyArray(Raster(inYTC))


# print arr_ytc
inComp = Raster(defineGDBpath(['ancillary','temp'])+'composite')
# self.arr_comp = arcpy.RasterToNumPyArray(Raster(inComp))
inTraj = Raster(defineGDBpath(['refine','trajectories'])+'traj_ytc30_2008to2016')


class NibbleObject:

    def __init__(self, mmu, res, years, subtype):
        self.res = res
        self.mmu = mmu
        
        self.years = years
        self.subtype = subtype

        self.datarange = str(self.years[0])+'to'+str(self.years[1])

   #      if self.years[1] == 2016:
			# self.datarange = str(self.years[0])+'to'+str(self.years[1]-1)
			# print 'self.datarange:', self.datarange

   #      else:
			# self.datarange = str(self.years[0])+'to'+str(self.years[1])
			# print 'self.datarange:', self.datarange
        
   #      if self.subtype == 'mtr':
   #      	self.gdb_path = defineGDBpath(['core', 'mmu'])
	  #       self.raster_name = 'traj_cdl'+self.res+'_b_'+self.datarange+'_rfnd_n8h_mtr'
	  #       self.in_raster = defineGDBpath(['core', 'mtr']) + self.raster_name
	  #       self.mask_name = self.raster_name + '_8w_msk' + self.mmu
	  #       self.in_mask_raster = self.gdb_path + self.mask_name
	  #       self.out_fishnet = defineGDBpath(['ancillary', 'temp']) + 'fishnet_' + self.subtype
	  #       self.pixel_type = "8_BIT_UNSIGNED"
   #      else:
			# self.gdb_path = defineGDBpath(['post', self.subtype])
			# self.raster_name = self.subtype+self.res+'_'+self.datarange+'_mmu'+self.mmu
			# self.in_raster = self.gdb_path + self.raster_name
			# print 'yo', self.in_raster
			# self.mask_name = self.raster_name + '_clean'
			# self.in_mask_raster = self.gdb_path + self.mask_name
        self.out_fishnet = defineGDBpath(['ancillary', 'temp']) + 'fishnet_' + self.subtype
			# self.pixel_type = "16_BIT_UNSIGNED"
        
  #       self.inYTC = Raster(defineGDBpath(['refine','ytc'])+'ytc30_2008to2016')
		# # self.arr_ytc = arcpy.RasterToNumPyArray(Raster(inYTC))


		# # print arr_ytc
  #       self.inComp = defineGDBpath(['ancillary','temp'])+'composite'
		# # self.arr_comp = arcpy.RasterToNumPyArray(Raster(inComp))
  #       self.inTraj = defineGDBpath(['refine','trajectories'])+'traj_ytc30_2008to2016'
		# self.arr_traj = arcpy.RasterToNumPyArray(Raster(inTraj))


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
	arcpy.env.snapRaster = inYTC
	arcpy.env.cellsize = inYTC
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	mask = np.zeros((13789, 21973), dtype=np.int)



	# inYTC = Raster(defineGDBpath(['refine','ytc'])+'ytc30_2008to2016')
	# arr_ytc = arcpy.RasterToNumPyArray(in_raster=inYTC, lower_left_corner = arcpy.Point(inYTC.extent.XMin,inYTC.extent.YMin), ncols = 13789, nrows = 21973, nodata_to_value = 0)
	arr_ytc = arcpy.RasterToNumPyArray(in_raster=inYTC, lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)
	print 'art',arr_ytc
	#composite stack of clds

	# self.inComp = defineGDBpath(['ancillary','temp'])+'composite'
	# # self.arr_comp = arcpy.RasterToNumPyArray(Raster(inComp))
	# self.inTraj = defineGDBpath(['refine','trajectories'])+'traj_ytc30_2008to2016'
	# # inComp = Raster(defineGDBpath(['ancillary','temp'])+'composite_fish')


	# inComp = Raster(defineGDBpath(['ancillary','temp'])+'composite')
	arr_comp = arcpy.RasterToNumPyArray(in_raster=inComp, lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)


	# inTraj = Raster(defineGDBpath(['refine','trajectories'])+'traj_ytc30_2008to2016')
	arr_traj = arcpy.RasterToNumPyArray(in_raster=inTraj, lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)

	#get the trajectory values that satify the condtion from postgres
	# rows=createReclassifyList_mod()
	# print 'rows------', rows

	# thelist = (arr_traj == 5392).nonzero()
    
	#find the location of each pixel labeled with specific arbitray value in the rows list  
	for row in rows:
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
	            print '-----------------same--------------------------------'
	            print bandindexlist
	            print 'x:',x[0]
	            print 'y:',x[1]
	            mask[x[0],x[1]] = 1





	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
 	
 	


	ras_out = arcpy.NumPyArrayToRaster(mask,x_cell_size=30, y_cell_size=30, value_to_nodata=0)
	

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

	arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_path, nbl_raster, Raster(nibble.in_raster).spatialReference, nibble.pixel_type, nibble.res, "1", "LAST","FIRST")

	##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(nibble.gdb_path + nbl_raster, "Overwrite")

	## Overwrite pyramids
	gen.buildPyramids(nibble.gdb_path + nbl_raster)



def createReclassifyList_mod():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
   
    # query = (
    # "SELECT DISTINCT \"Value\" "
    # "FROM refinement.traj_"+refine.name+refine.res+"_"+refine.datarange+" "
    # "WHERE 122 = traj_array[1] "
    # "OR 123 = traj_array[1] "
    # "OR 124 = traj_array[1] "
    # "OR 61 = traj_array[2] "
    # "OR '{37,36,36}' = traj_array "
    # "OR '{152,36,36}' = traj_array "
    # "OR '{176,36,36}' = traj_array"
    # )

    query = (
    "SELECT DISTINCT \"Value\" "
    "FROM refinement.traj_"+nibble.subtype+nibble.res+"_"+nibble.datarange+" "
    "WHERE 61 = traj_array[2] "
    )

    print query

    cur.execute(query)
    #create empty list
    fulllist=[[0,0,"NODATA"]]

    # fetch all rows from table
    rows = cur.fetchall()
    return rows
    print 'number of records in lookup table', len(rows)
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    # for row in rows:
    #     templist=[]
    #     templist.append(row[0])
    #     templist.append(refine.traj_change)
    #     fulllist.append(templist)
    # print fulllist
    # return fulllist




### Define conversion object ######
nibble = NibbleObject(
	  #mmu
	  '5',
	  #resolution
	  '30',
	  #data-range
	  [2008,2016],
	  #subtype
	  'ytc'
      )

rows=createReclassifyList_mod()
print rows


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
    
	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=5)
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join

	mosiacRasters()
    
