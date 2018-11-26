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


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

print 'this is'
cur = conn.cursor()

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


# def createReclassifyList(data):
# 	# cur = conn.cursor()

# 	query = "SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])
# 	print 'query:', query

# 	cur.execute(query)
# 	#create empty list
# 	fulllist=[[0,0,"NODATA"]]

# 	### fetch all rows from table
# 	rows = cur.fetchall()
# 	# print rows
# 	# print 'number of records in lookup table', len(rows)
# 	return rows






# def getNonCropList():
	

# 	query = "SELECT value FROM misc.lookup_cdl WHERE b = '0'"
# 	# print 'query:', query

# 	cur.execute(query)

# 	### fetch all rows from table
# 	rows = cur.fetchall()

# 	### use list comprehension to convert list of tuples to list
# 	noncrop_list = [i[0] for i in rows]
# 	print 'noncrop_list:', noncrop_list
	
# 	### add 36 and 61 to noncrop list!!!
# 	return noncrop_list + [36,61]




# def getCropList():
# 	# cur = conn.cursor()

# 	query = "SELECT value FROM misc.lookup_cdl WHERE b = '1' and value NOT IN (36, 61) ORDER BY value"
# 	print 'query:', query

# 	cur.execute(query)

# 	### fetch all rows from table
# 	rows = cur.fetchall()

# 	### use list comprehension to convert list of tuples to list
# 	crop_list = [i[0] for i in rows]
# 	print 'crop_list:', crop_list
	
# 	### add 36 and 61 to noncrop list!!!
# 	return crop_list

		



def getPixelsPerTile(vector):
	pixels_cols=153811
	pixels_rows=96523

	fishnet_values={"mask_2007":{"rws":7,"cls":49}}




	if vector=='rws':
		return pixels_rows / fishnet_values['mask_2007'][vector]
	elif vector=='cls':
		return pixels_cols / fishnet_values['mask_2007'][vector]





cls = getPixelsPerTile('cls')
print 'cls',cls
rws = getPixelsPerTile('rws')
print 'rws',rws

	


def execute_task(args):
	in_extentDict, data = args

	fc_count = in_extentDict[0]
	print 'fffffffffffffff', fc_count
	
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
	
	# arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	states = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	traj_rfnd = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v6\\v6_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v6', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	ytc = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s25\\post\\ytc_s25.gdb\\s25_ytc30_2008to2017_mmu5', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	


	
	arbtraj_list = []
	ytc_list = []
	tile_list = []
	# mask_list = []
	states_list = []
	traj_array_list = []

	indices = np.where(np.logical_and(np.greater(ytc,2008),np.less(ytc,2017)))

	#stack indices so easier to work with
	stacked_indices=np.column_stack((indices[0],indices[1]))


	for pixel_location in stacked_indices:
		row = pixel_location[0] 
		col = pixel_location[1]



		ytx = ytc[row][col]
		# print 'ytx', ytx

		#year before conversion for either expansion or abandonment
		ybx = ytx-1
		# print 'ybx', ybx


	    ############################  get cdl value of ybx and ytx  ##################################
		#get the pixel value for ybx
		pixel_value_ybx =  cdls[ybx][row][col]
		#print 'pixel_value_ybx', pixel_value_ybx

		#get the pixel value for ytx
		pixel_value_ytx =  cdls[ytx][row][col]
		#print 'pixel_value_ytx', pixel_value_ytx 




        ############################  get series list  ##################################

		#### find the years stil before in the time series for this pixel location
		years_before_list = [i for i in data['global']['years'] if i < ytx]
		#print 'years_before_list', years_before_list
		list_before = []
		for year in years_before_list:
			list_before.append(cdls[year][row][col])
		

		#### find the alue of the years after cy!?!
		years_after_list = [i for i in data['global']['years'] if i >= ytx]
		#print 'years_after_list', years_after_list
		list_after = []	
		for year in years_after_list:
			list_after.append(cdls[year][row][col])
		# print 'list_after', list_after

		list_entire=list_before+list_after

	    ############################  append values to lists  ##################################
		tile_list.append(fc_count)
		arbtraj_list.append(traj_rfnd[row][col])
		ytc_list.append(ytc[row][col])
		states_list.append(states[row][col])
		traj_array_list.append(str(list_entire))
	






	##merge the lists into a tuple 
	data_tuples = list(zip(tile_list, arbtraj_list,states_list,ytc_list,traj_array_list))

	df=pd.DataFrame(data_tuples, columns=['tile', 'traj', 'state', 'ytc', 'traj_array'])

	del cdls, states, traj_rfnd, ytc, tile_list, arbtraj_list, states_list, ytc_list, traj_array_list, data_tuples
	
	if df.empty:
		pass
	else:
		df.to_sql('tile_{}'.format(str(fc_count)), engine, schema='qaqc_tiles_t2', if_exists='replace')

	del df
	

	


    



def run(data):
	print "mask mutiple masks-----------+++++++++++++++++"
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	# traj_list = createReclassifyList(data)

	# noncroplist = getNonCropList()

	# croplist = getCropList()

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_mask_dev_36_61', ["oid","SHAPE@"]):
	
	# for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_mtr_temp', ["oid","SHAPE@"]):
	# for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_mtr'], ["oid","SHAPE@"]):
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
	pool = Pool(processes=11)
	pool.map(execute_task, [(ed, data) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data)


if __name__ == '__main__':
	run(data)
   
