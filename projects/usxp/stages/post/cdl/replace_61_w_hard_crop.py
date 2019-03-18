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


# cls = getPixelsPerTile('cls')

# print 'cls',cls

# rws = getPixelsPerTile('rws')

# print 'rws',rws



def run(data, yxc_inraster, subtype, subtype_inraster, XMin, YMin, XMax, YMax, croplist_subset):

	arcpy.env.snapRaster = data['pre']['traj']['path']

	arcpy.env.cellsize = data['pre']['traj']['path']

	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	

	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	rws = 13789

	cls = 21973


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

	#### convert arcgis raster from the parent function to a numpy raster!!!  (brilliant step)
	yxc_numpy = arcpy.RasterToNumPyArray(in_raster=yxc_inraster, lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	yxc_inraster = None

	# print subtype_inraster
	subtype_inraster_numpy = arcpy.RasterToNumPyArray(in_raster=subtype_inraster, lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	subtype_inraster = None

	print 'datatype:', subtype_inraster_numpy.dtype

	#####Not sure what this does but might set the raster to unsigned 8 bit integer
	subtype_inraster_numpy.astype(np.uint8)

	print 'datatype:', subtype_inraster_numpy.dtype

	print 'np.amax(subtype_inraster_numpy)', np.amax(subtype_inraster_numpy)

	nodata = np.amax(subtype_inraster_numpy)


	#####get the index of all pixels with value 61 in the raster
	indices = np.where(subtype_inraster_numpy == 61)

	######stack indices so easier to work with
	stacked_indices=np.column_stack((indices[0],indices[1]))

	####for each pixel perform refinement
	for pixel_location in stacked_indices:

		###get the coordinate for the pixel
		row = pixel_location[0] 
		col = pixel_location[1]

		###get the year from the yxc dataset
		year = yxc_numpy[row][col]


		####get the years before conversion
		years_before_list = [i for i in data['global']['years'] if i < year]
		
		####get the cdl values for each of the years in years_before_list
		list_before = []
		for year in years_before_list:
			list_before.append(cdls[year][row][col])



		####get the years after conversion
		years_after_list = [i for i in data['global']['years'] if i > year]

		####get the cdl values for each of the years in years_after_list
		list_after = []	
		for year in years_after_list:
			list_after.append(cdls[year][row][col])


		####the entire list of cdl values
		list_entire=list_before+list_after


		##### check to see if there is a crop before/after 61 depending if the subtype is
		if subtype == 'fc':
			####if the list_after has no value cld values in the croplist(minus 61) then reclass pixel as null
			if not any(np.isin(list_after, croplist_subset)):
				subtype_inraster_numpy[row][col] = np.amax(subtype_inraster_numpy)

			####if the list_after has a value cld values in the croplist(minus 61) then reclass pixel with the first crop value after conversion
			else:	
				####get the index of the first true in where condition	
				first_index_true = np.where(np.isin(list_after, croplist_subset))[0][0]
				####get cdl value from the first_index_true
				subtype_inraster_numpy[row][col] = list_after[first_index_true]

			

		elif subtype == 'bfnc':
			####if the list_before has no cld values other than 61 then keep the pixel value as it is.(note these pixels should always convert to development!?!)
			if not any(np.isin(list_before, croplist_subset)):
				pass

			####if the list_before has a value cld values in the croplist(minus 61) then reclass pixel with the last crop value before conversion
			else:
				####get the index of the last true in where condition	
				last_index_true = np.where(np.isin(list_before[::-1], croplist_subset))[0][0]
				####get cdl value using first_index_true
				subtype_inraster_numpy[row][col] = list_before[::-1][last_index_true]

			











	####convert the numpy array after refinement is don back to main funtion as a raster object 
	refined_subtype_inraster = arcpy.NumPyArrayToRaster(subtype_inraster_numpy, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=65535)

	del cdls, yxc_numpy, subtype_inraster_numpy, indices, stacked_indices, years_before_list, years_after_list, list_before, list_after, list_entire

	return refined_subtype_inraster







if __name__ == '__main__':

	run(data, yxc_inraster, subtype, subtype_inraster, XMin, YMin, XMax, YMax, croplist_subset)