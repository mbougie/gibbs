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


engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')


arcpy.CheckOutExtension("Spatial")
conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
cur = conn.cursor()



arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def getNumpyarray(rg):

    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(rg)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(rg,fields)
    print arr
    return arr
    
    # # convert numpy array to pandas dataframe
    # df = pd.DataFrame(data=arr)

    # df.columns = map(str.lower, df.columns)

    # print 'df-----------------------', df

    # total=df['count'].sum()
    
    # # # # use pandas method to import table into psotgres
    # df.to_sql(data['post'][yxc]['filename'], engine, schema='counts_yxc', if_exists='replace')
    
    # # #add trajectory field to table
    # addAcresField('counts_yxc', data['post'][yxc]['filename'], yxc, '30', total)





def getCropList():
	# cur = conn.cursor()

	query = "SELECT value FROM misc.lookup_cdl WHERE b = '1' and value NOT IN (36, 61) ORDER BY value"
	print 'query:', query

	cur.execute(query)

	### fetch all rows from table
	rows = cur.fetchall()

	### use list comprehension to convert list of tuples to list
	crop_list = [i[0] for i in rows]
	# ##print 'noncrop_list:', noncrop_list
	
	### add 36 and 61 to noncrop list!!!
	return crop_list






def getPixelsPerTile(vector):
	pixels_cols=153811
	pixels_rows=96523

	fishnet_values={"mask_2007":{"rws":7,"cls":49}}

	if vector=='rws':
		return pixels_rows / fishnet_values['mask_2007'][vector]
	elif vector=='cls':
		return pixels_cols / fishnet_values['mask_2007'][vector]





cls = getPixelsPerTile('cls')
##print 'cls',cls
rws = getPixelsPerTile('rws')
##print 'rws',rws

	


def execute_task(args):


	in_extentDict, data, croplist = args

	fc_count = in_extentDict[0]
	
	procExt = in_extentDict[1]
	# ##print procExt
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


	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((rws, cls), dtype=np.uint8)
    
    ### create numpy arrays for input datasets cdls and traj
	cdls = {
			2008:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2008', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2009:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2009', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2010:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2010', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2011:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2011', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2012:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2012', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2013:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2013', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2014:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2014', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2015:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2015', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2016:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2016', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
			2017:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	       }
	
	arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	rg = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s22\\core\\core_s22.gdb\\s22_mtr4_id', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)

	# ##find the location of each pixel labeled with specific arbitray value in the rows list  
    
	# print np.unique(rg)
	for patch in  np.unique(rg)[1:]:
		"print test---"
		print '---------------patch-----out------------------', patch

		# 	#Return the indices of the pixels that have values of the ytc arbitrsy values of the traj.
		indices = (rg == patch).nonzero()
		# print indices

		# 	#stack indices so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))

		patch_list = []

		#get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:

			row = pixel_location[0] 
			col = pixel_location[1]

			patch_list.append(arr_traj[row][col])
		


		keep = ~np.isnan(patch_list)
		y = np.bincount(keep)
		ii = np.nonzero(y)[0]
		yo=np.vstack((ii,y[ii])).T
		# print yo












				#print 'pixel_value_ybx', pixel_value_ybx

		# 		#get the pixel value for ytx
		# 		pixel_value_ytx =  cdls[ytx][row][col]
		# 		#print 'pixel_value_ytx', pixel_value_ytx 

		# 		#### find the years stil before in the time series for this pixel location
		# 		years_before_list = [i for i in data['global']['years'] if i < ytx]
		# 		#print 'years_before_list', years_before_list
		# 		list_before = []
		# 		for year in years_before_list:
		# 			list_before.append(cdls[year][row][col])
		# 		#print 'list_before', list_before


		# 		#### find the alue of the years after cy!?!
		# 		years_after_list = [i for i in data['global']['years'] if i > ytx]
		# 		#print 'years_after_list', years_after_list
		# 		list_after = [pixel_value_ytx]	
		# 		for year in years_after_list:
		# 			list_after.append(cdls[year][row][col])
		# 		#print 'list_after', list_after

		# 		list_entire=list_before+list_after
		# 		print 'list_entire------------------------------------------->', list_entire
		# 		print 'list_entire length', len(list_entire)

		# 		# pixel_list=[traj_value,ytx,list_entire]
		# 		pixel_list=[traj_value,ytx]

		# 		full_list.append(pixel_list)










	# df = pd.DataFrame(full_list, columns=['traj','yfc'])


	# df.to_sql("test_df", engine, schema='qaqc')

	# arcpy.ClearEnvironment("extent")

	# outname = "tile_" + str(fc_count) +'.tif'

	# # #create
	# outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

	# # NumPyArrayToRaster (in_array, {lower_left_corner}, {x_cell_size}, {y_cell_size}, {value_to_nodata})
	# myRaster = arcpy.NumPyArrayToRaster(outData, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=0)

	# ##free memory from outdata array!!
	# outData = None

	# myRaster.save(outpath)

	# myRaster = None



def mosiacRasters(data):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])
	print 'inTraj:', inTraj


	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['refine']['gdb'], data['refine']['mask_dev_alfalfa_fallow_yfc']['filename'], inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['refine']['mask_dev_alfalfa_fallow_yfc']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['refine']['mask_dev_alfalfa_fallow_yfc']['path'])



    



def run(data):
	##print "mask mutiple masks-----------"
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	# traj_list = createReclassifyList(data)
	# print 'traj_list', traj_list

	croplist = getCropList()
	print 'croplist:', croplist

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_mask_dev_36_61_test', ["oid","SHAPE@"]):
	
	# for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_mtr_temp', ["oid","SHAPE@"]):
	# for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_mtr'], ["oid","SHAPE@"]):
		atlas_stco = row[0]
		##print atlas_stco
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[atlas_stco] = ls

	##print 'extDict', extDict
	##print'extDict.items',  extDict.items()

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=1)
	pool.map(execute_task, [(ed, data, croplist) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data)


if __name__ == '__main__':
	run(data)
   
