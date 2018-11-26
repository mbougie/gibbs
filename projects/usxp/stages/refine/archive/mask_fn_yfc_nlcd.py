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



def getPixelsPerTile(vector):
	pixels_cols=161190
	pixels_rows=104424

	fishnet_values={"mask_2007":{"rws":19,"cls":6}}

	if vector=='rws':
		return pixels_rows / fishnet_values['mask_2007'][vector]
	elif vector=='cls':
		return pixels_cols / fishnet_values['mask_2007'][vector]

cls = getPixelsPerTile('cls')
print 'cls',cls
rws = getPixelsPerTile('rws')
print 'rws',rws


###NOTE STILL HAVE TO DEAL WITH YFC IN QUERY BELOW  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def createReclassifyList(data):
	cur = conn.cursor()
	
	query = "SELECT \"Value\", mtr, a.traj_array from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE mtr=1 or mtr=5".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])
	print 'query:', query

	cur.execute(query)
	#create empty list
	fulllist=[[0,0,"NODATA"]]

	# fetch all rows from table
	rows = cur.fetchall()
	print rows
	print 'number of records in lookup table', len(rows)
	return rows
	

def execute_task(args):
	in_extentDict, data, traj_list = args

	fc_count = in_extentDict[0]
	
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((rws, cls), dtype=np.uint16)
    
    ### create numpy arrays for input datasets nlcds and traj
	nlcds = {
			1992:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_1992', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2001:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2001', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2006:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2006', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2011:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2011', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			}
	
	arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)

	#### find the location of each pixel labeled with specific arbitray value in the rows list  
	#### note the traj_list is derived from the sql query above
	for row in traj_list:
		traj_value = row[0]
		mtr = row[1]
		traj_array = row[2]

		### remove the first for elements from traj_array
		traj_array_trunc = traj_array[4:]

		
		traj_mtr1_dict = {data['refine']['dict_arb_yfc']['2012']:[0,0,0,1,0,0,0,0,0,0], data['refine']['dict_arb_yfc']['2013']:[0,0,0,0,1,0,0,0,0,0], data['refine']['dict_arb_yfc']['2014']:[0,0,0,0,0,1,0,0,0,0], data['refine']['dict_arb_yfc']['2015']:[0,0,0,0,0,0,1,0,0,0], data['refine']['dict_arb_yfc']['2016']:[0,0,0,0,0,0,0,1,0,0]}

        #### create a distionary contain specific hard trajectories considered conversion with associated yewar in the key of the dictionary
		traj_trunc_dict = {data['refine']['dict_arb_yfc']['2012']:[0,0,0,0,0,0], data['refine']['dict_arb_yfc']['2013']:[1,0,0,0,0,0], data['refine']['dict_arb_yfc']['2014']:[1,1,0,0,0,0], data['refine']['dict_arb_yfc']['2015']:[1,1,1,0,0,0], data['refine']['dict_arb_yfc']['2016']:[1,1,1,1,0,0]}

		#Return the indices of the pixels that have values of the ytc arbitray values of the traj.
		indices = (arr_traj == traj_value).nonzero()

		#stack the indices variable above so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))
        
        #####  get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:

			# print traj_mtr1_dict
			# print(mtr)
			# print traj_array_trunc
			# print (traj_array_trunc in traj_trunc_dict.values())
			row = pixel_location[0] 
			col = pixel_location[1]
            
			nlcd_list = []
			nlcd_list.append(nlcds[2006][row][col])
			nlcd_list.append(nlcds[2011][row][col])

			######  get the length of nlcd list containing only the value 81 and 82
			##81 = pasture/hay
			##82 = cultivated crop
			count_82 = nlcd_list.count(82)

			###  if NLCD both years and mtr=1 then treat pixel as conversion
			if (count_82 == 2) and (mtr == 1) and (traj_array in traj_mtr1_dict.values()):
				current_key = traj_mtr1_dict.keys()[traj_mtr1_dict.values().index(traj_array)]
				# print current_key
				outData[row,col] = current_key


			###  if NLCD both years and mtr=5 and traj_array_trunc is one of the values in traj_trunc_dict dictionary then treat pixel as conversion
			if (count_82 == 2) and (mtr == 5) and (traj_array_trunc in traj_trunc_dict.values()):
				# print(mtr)
				current_key = traj_trunc_dict.keys()[traj_trunc_dict.values().index(traj_array_trunc)]
				# print(current_key)
				outData[row,col] = current_key



    

	arcpy.ClearEnvironment("extent")

	outname = "tile_" + str(fc_count) +'.tif'

	# #create
	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	# NumPyArrayToRaster (in_array, {lower_left_corner}, {x_cell_size}, {y_cell_size}, {value_to_nodata})
	myRaster = arcpy.NumPyArrayToRaster(outData, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=0)

	##free memory from outdata array!!
	outData = None

	myRaster.save(outpath)

	myRaster = None





def mosiacRasters(data):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj


	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['refine']['gdb'], data['refine']['mask_addYFC2traj']['filename'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['refine']['mask_addYFC2traj']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_addYFC2traj']['path'])




    





def run(data):
	print "mask nlcd------------"
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data)

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_nlcd', ["oid","SHAPE@"]):
		atlas_stco = row[0]
		print atlas_stco
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[atlas_stco] = ls

	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=3)
	pool.map(execute_task, [(ed, data, traj_list) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(data)


if __name__ == '__main__':
	run(data)
   

    
   
