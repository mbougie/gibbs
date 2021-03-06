import sys
import os
#import modules from other folders
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import arcpy
from arcpy import env
from arcpy.sa import *
import glob

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
import general as gen
import json
import fnmatch
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import multiprocessing




arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



###make this a general function
def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\current_instance.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



##create global objects to reference through the script

data = getJSONfile()





def execute_task(in_extentDict):

	fc_count = in_extentDict[0]
	
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	#The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	cls = 21973
	rws = 13789


	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((13789, 21973), dtype=np.int)
    
    ### create numpy arrays for input datasets cdls and traj
	
	change=arcpy.RasterToNumPyArray(in_raster='D:\\projects\\usxp\\series\\s14\\core\\core.gdb\\s14_traj_cdl30_b_2008to2016_rfnd_n8h_mtr_8w_mmu5', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
	conf=arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\conf.gdb\\National_average_cdl_30m_r_2008to2016_albers_confidence', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)
	     
	# np.nonzero(change)
	# # arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)

	# # find the location of each pixel labeled with specific arbitray value in the rows list  
	# for row in location_list:
	# 	#year of conversion for either expansion or abandonment
	# 	ytx = row[1]
	# 	print 'ytx', ytx
		
	# 	#year before conversion for either expansion or abandonment
	# 	ybx = row[1]-1
	# 	print 'ybx', ybx

	#Return the indices of the pixels that have values of the ytc arbitrsy values of the traj.
	indices = np.nonzero(change)
	print indices

	# #stack indices so easier to work with
	# stacked_indices=np.column_stack((indices[0],indices[1]))
        
 #        #get the x and y location of each pixel that has been selected from above
	# for pixel_location in stacked_indices:
	# 	row = pixel_location[0] 
	# 	col = pixel_location[1]
 #        print 'row', row 
 #        print 'col', col


  #       #get the pixel value for ytx
		# pixel_value_ytx =  cdls[ytx][row][col]
		# #get the pixel value for ybx
		# pixel_value_ybx =  cdls[ybx][row][col]
  #       print row
		# #####  create dev mask componet
		# # cdls
		# if np.isnan(change[row,col]):
		# 	print 'null'
		# 	break
		# else:
		# 	outData[row,col] = conf[row,col]

  #       #####  create 36_61 mask componet
		# if pixel_value_ytx in [36,61]:
		# 	#find the years stil left in the time series for this pixel location
		# 	yearsleft = [i for i in data['global']['years'] if i > ytx]

  #           #only focus on the extended series ---dont care about 2012
		# 	if len(yearsleft) > 1:
		# 		#create templist to hold the rest of the cld values for the time series.  initiaite it with the first cdl value
		# 		templist = [pixel_value_ytx]
		# 		for year in yearsleft:
		# 			# print 'year', year
		# 			# print 'cdls[year][row][col] :', cdls[year][row][col]
		# 			templist.append(cdls[year][row][col])

		# 		#check if all elements in array are the same
		# 		if len(set(templist)) == 1:
		# 			outData[row,col] = data['refine']['mask_dev_alfalfa_fallow']['arbitrary']




	arcpy.ClearEnvironment("extent")

	outname = "tile_" + str(fc_count) +'.tif'

	# #create
	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	# NumPyArrayToRaster (in_array, {lower_left_corner}, {x_cell_size}, {y_cell_size}, {value_to_nodata})
	myRaster = arcpy.NumPyArrayToRaster(outData, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=0)
	

	myRaster.save(outpath)





def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj


	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['refine']['gdb'], data['refine']['mask_dev_alfalfa_fallow']['filename'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['refine']['mask_dev_alfalfa_fallow']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_dev_alfalfa_fallow']['path'])



    





# def run():  
if __name__ == '__main__':
    #######clear the tiles from directory
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['counties_subset'], ["SHAPE@"]):
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

	# mosiacRasters()


# run()
    
   
