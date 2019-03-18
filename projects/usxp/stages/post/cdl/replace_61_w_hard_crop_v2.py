
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

print 'this is'
cur = conn.cursor()

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


# def getPixelsPerTile(vector):
# 	pixels_cols=153811
# 	pixels_rows=96523

# 	fishnet_values={"mask_2007":{"rws":7,"cls":49}}

# 	if vector=='rws':
# 		return pixels_rows / fishnet_values['mask_2007'][vector]
# 	elif vector=='cls':
# 		return pixels_cols / fishnet_values['mask_2007'][vector]



def getCroplist():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = "SELECT value from misc.lookup_cdl where b='1' and value <> 61"
	# print 'query:', query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[]
	for index, row in df.iterrows():
	    value=row['value'] 
	    fulllist.append(value)
	print 'fulllist: ', fulllist
	return fulllist


# cls = getPixelsPerTile('cls')
# print 'cls',cls
# rws = getPixelsPerTile('rws')
# print 'rws',rws


# XMin, YMin, XMax, YMax

def run(data, yxc_inraster, inraster, XMin, YMin, XMax, YMax):

	croplist_subset = getCroplist()

	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	rws = 13789
	cls = 21973


# 21973, 13789

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

	# ytc = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s25\\post\\ytc_s25.gdb\\s25_ytc30_2008to2017_mmu5', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	ytc = arcpy.RasterToNumPyArray(in_raster=yxc_inraster, lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	yxc_inraster = None

	# print inraster
	inraster_numpy = arcpy.RasterToNumPyArray(in_raster=inraster, lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	inraster = None
	# print 'datatype:', x.dtype
	# print 'np.amax(inraster_numpy)', np.amax(inraster_numpy)

	inraster_numpy.astype(np.uint8)
	# print 'datatype:', x.dtype
	# print 'np.amax(inraster_numpy)', np.amax(inraster_numpy)

	indices = np.where(inraster_numpy == 61)

	#stack indices so easier to work with
	stacked_indices=np.column_stack((indices[0],indices[1]))


	for pixel_location in stacked_indices:
		row = pixel_location[0] 
		col = pixel_location[1]

		year = ytc[row][col]
		# print '---------------  new -------------------------------------------'
		# print 'year', year

		# print 'inraster_numpy[row][col]', inraster_numpy[row][col]

		pixel_value_ytx =  cdls[year][row][col]
		# print 'pixel_value_ytx', pixel_value_ytx

		years_before_list = [i for i in data['global']['years'] if i < year]
		# print 'years_before_list', years_before_list

		list_before = []
		for year in years_before_list:
			list_before.append(cdls[year][row][col])
		# print 'list_before:', list_before

		#### find the value of the years after cy!?!
		years_after_list = [i for i in data['global']['years'] if i > year]
		# print 'years_after_list', years_after_list

		list_after = []	
		for year in years_after_list:
			list_after.append(cdls[year][row][col])
		# print 'list_after:', list_after

		list_entire=list_before+list_after
		# print 'list_entire:', list_entire


		# print 'croplist_subset:', croplist_subset
		# print np.where(np.isin(list_after, croplist_subset))
		if len(list_after) > 0:
			first_index_true = np.where(np.isin(list_after, croplist_subset))[0][0]

		else:
			print('length of lsit after is 0')
			print 'list_entire:', list_entire
			# print 'first_index_true: ', first_index_true
			# print 'list_after[first_index_true]', list_after[first_index_true]
			# inraster_numpy[row][col] = list_after[first_index_true]

		# if not any(np.isin(list_after, croplist_subset)):

		# 	print 'year', year

		# 	print 'list_before:', list_before

		# 	print 'list_after:', list_after

		# 	print 'list_entire:', list_entire
		# 	print 'HIIIIIIIIIIIIIIIIIIIIIIIIIIIIII  this is it?????'; np.isin(list_after, croplist_subset)

		# 	print 'np.amax(inraster_numpy)', np.amax(inraster_numpy)
		# 	inraster_numpy[row][col] = np.amax(inraster_numpy)



		# else:	
		# 	first_index_true = np.where(np.isin(list_after, croplist_subset))[0][0]
		# 	# print 'first_index_true: ', first_index_true
		# 	# print 'list_after[first_index_true]', list_after[first_index_true]
		# 	inraster_numpy[row][col] = list_after[first_index_true]







	####convert the numpy array after refinement is done back to main funtion as a raster object 
	refined_inraster = arcpy.NumPyArrayToRaster(inraster_numpy, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=65535)
	
	del cdls, ytc, inraster_numpy, indices, stacked_indices
	return refined_inraster



if __name__ == '__main__':

	run(data, yxc_inraster, inraster, XMin, YMin, XMax, YMax)
	# run(data, yxc_inraster, inraster, XMin, YMin, XMax, YMax)
   
