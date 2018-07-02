
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







def run(inraster, XMin, YMin, series_years, path_mtr):

	arcpy.env.snapRaster = path_mtr
	arcpy.env.cellsize = path_mtr
	arcpy.env.outputCoordinateSystem = path_mtr

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

	traj_rfnd = arcpy.RasterToNumPyArray(in_raster='C:\\Users\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\traj_rfnd\\v4_traj_cdl30_b_2008to2017_rfnd_v6', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)


	ytc = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s25\\post\\ytc_s25.gdb\\s25_ytc30_2008to2017_mmu5', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	
	indices = (ytc > 0).nonzero()

	#stack indices so easier to work with
	stacked_indices=np.column_stack((indices[0],indices[1]))


	for pixel_location in stacked_indices:
		row = pixel_location[0] 
		col = pixel_location[1]


		print ytc[row][col]
		print traj_rfnd[row][col]
		

		# year = ytc[row][col]
		# # print 'year', year

		# # print 'year', type(year)
		# # print 'year', year
		# # year = int(year)
		# # print 'year', type(year)
		# # print 'year', year


		# # print 'series_year', type(series_years)
		# # print 'series_year', series_years

		# years_after_list = [i for i in series_years if i > year]

		# # print 'years_after_list', years_after_list
		# list_after = []	
		# for year in years_after_list:
		# 	list_after.append(cdls[year][row][col])

		# # print 'list_after', list_after

		# inraster_numpy[row][col] = 255


		########   !!!!!!!!!!!!!!!!!!!!!!!!!!!   !!!!!!!!!!!!!!!!!   #######################################################
		#### get first value not 61 or noncrop and replace the inraster_numpy pixel with this value-------------->  then convert this numpy array back into a raster and return to the main function



	# refined_inraster = arcpy.NumPyArrayToRaster(inraster_numpy, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=0)
	# inraster_numpy = None
	# return refined_inraster



if __name__ == '__main__':
	run(inraster, XMin, YMin, series_years, path_mtr)
   