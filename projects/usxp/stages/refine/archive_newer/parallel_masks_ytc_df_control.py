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


def createReclassifyList(data):
	# cur = conn.cursor()

	query = "SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])
	print 'query:', query

	cur.execute(query)
	#create empty list
	fulllist=[[0,0,"NODATA"]]

	### fetch all rows from table
	rows = cur.fetchall()
	# print rows
	# print 'number of records in lookup table', len(rows)
	return rows






def getNonCropList():
	

	query = "SELECT value FROM misc.lookup_cdl WHERE b = '0'"
	# print 'query:', query

	cur.execute(query)

	### fetch all rows from table
	rows = cur.fetchall()

	### use list comprehension to convert list of tuples to list
	noncrop_list = [i[0] for i in rows]
	print 'noncrop_list:', noncrop_list
	
	### add 36 and 61 to noncrop list!!!
	return noncrop_list + [36,61]




def getCropList():
	# cur = conn.cursor()

	query = "SELECT value FROM misc.lookup_cdl WHERE b = '1' and value NOT IN (36, 61) ORDER BY value"
	print 'query:', query

	cur.execute(query)

	### fetch all rows from table
	rows = cur.fetchall()

	### use list comprehension to convert list of tuples to list
	crop_list = [i[0] for i in rows]
	print 'crop_list:', crop_list
	
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
print 'cls',cls
rws = getPixelsPerTile('rws')
print 'rws',rws

	


def execute_task(args):
	in_extentDict, data, traj_list, noncroplist, croplist = args

	fc_count = in_extentDict[0]
	
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	# traj_list = [(16, 2016), (24, 2011), (30, 2016), (31, 2016), (39, 2016), (45, 2014), (47, 2016), (53, 2013), (54, 2013), (55, 2013), (61, 2013), (63, 2015), (74, 2015), (82, 2015), (83, 2016), (87, 2014), (94, 2014), (96, 2014), (100, 2014), (104, 2015), (106, 2016), (133, 2009), (136, 2009), (139, 2010), (141, 2016), (144, 2016), (152, 2016), (153, 2015), (158, 2009), (163, 2016), (164, 2015), (169, 2016), (174, 2009), (175, 2009), (176, 2009), (189, 2014), (193, 2016), (194, 2015), (195, 2014), (204, 2010), (206, 2012), (211, 2013), (220, 2010), (222, 2016), (228, 2009), (229, 2014), (234, 2014), (240, 2009), (241, 2009), (243, 2010), (244, 2013), (245, 2010), (254, 2010), (256, 2015), (263, 2010), (264, 2010), (280, 2010), (281, 2009), (285, 2015), (286, 2010), (288, 2013), (292, 2016), (293, 2009), (294, 2010), (297, 2013), (298, 2016), (300, 2015), (308, 2009), (310, 2010), (313, 2009), (314, 2009), (315, 2009), (316, 2013), (317, 2013), (325, 2009), (329, 2009), (333, 2015), (338, 2009), (342, 2010), (343, 2009), (351, 2016), (353, 2015), (354, 2012), (357, 2009), (358, 2010), (362, 2009), (363, 2013), (376, 2016), (380, 2016), (381, 2013), (384, 2015), (386, 2012), (394, 2013), (403, 2016), (404, 2010), (405, 2015), (408, 2010), (412, 2010), (415, 2012), (416, 2011), (417, 2011), (418, 2013), (423, 2011), (425, 2012), (431, 2011), (438, 2016), (447, 2016), (450, 2014), (452, 2009), (453, 2011), (459, 2011), (460, 2015), (462, 2009), (463, 2016), (466, 2009), (473, 2009), (477, 2013), (482, 2015), (496, 2016), (497, 2014), (501, 2009), (502, 2013), (505, 2010), (506, 2009), (508, 2013), (521, 2011), (522, 2009), (525, 2009), (530, 2014), (531, 2016), (532, 2012), (550, 2011), (551, 2010), (553, 2014), (555, 2016), (559, 2009), (561, 2009), (567, 2009), (573, 2010), (576, 2009), (580, 2009), (589, 2012), (596, 2014), (597, 2016), (600, 2009), (608, 2014), (611, 2013), (613, 2010), (617, 2013), (618, 2009), (621, 2015), (624, 2011), (634, 2015), (639, 2012), (640, 2009), (647, 2009), (650, 2009), (652, 2011), (653, 2010), (659, 2012), (660, 2015), (661, 2009), (668, 2011), (670, 2011), (671, 2012), (672, 2009), (675, 2010), (679, 2010), (684, 2014), (685, 2014), (687, 2011), (688, 2015), (689, 2009), (690, 2015), (694, 2014), (695, 2011), (698, 2012), (699, 2011), (702, 2009), (717, 2010), (723, 2012), (725, 2011), (727, 2015), (730, 2015), (733, 2011), (752, 2009), (756, 2010), (759, 2011), (762, 2016), (765, 2009), (766, 2009), (771, 2010), (772, 2013), (773, 2014), (775, 2014), (777, 2010), (778, 2015), (780, 2009), (785, 2013), (795, 2011), (800, 2014), (814, 2015), (816, 2013), (817, 2013), (821, 2011), (825, 2015), (840, 2011), (850, 2015), (852, 2014), (854, 2013), (858, 2012), (859, 2012), (869, 2012), (874, 2012), (882, 2010), (883, 2013), (884, 2011), (885, 2012), (887, 2015), (889, 2012), (891, 2009), (893, 2014), (897, 2014), (912, 2012), (918, 2014), (921, 2012), (927, 2012), (932, 2015), (933, 2015), (936, 2014), (937, 2015), (941, 2011), (942, 2011), (954, 2014), (957, 2011), (958, 2011), (972, 2012), (973, 2012), (974, 2011), (979, 2015), (999, 2011), (1002, 2011), (1003, 2012), (1007, 2012), (1012, 2011), (1018, 2011), (1020, 2012), (1022, 2012), (1024, 2009)]
	# noncrop_list = [62, 63, 64, 65, 81, 82, 190, 195, 176, 37, 87, 88, 92, 111, 112, 121, 131, 141, 143, 142, 122, 123, 152, 124, 83]
	# crop_list = [1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 66, 67, 68, 69, 70, 71, 72, 74, 75, 76, 77, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 254]



	#set environments
	#The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = data['pre']['traj']['path']
	arcpy.env.cellsize = data['pre']['traj']['path']
	arcpy.env.outputCoordinateSystem = data['pre']['traj']['path']	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	# outData = numpy.zeros((rows,cols), numpy.int16)
	# outData = np.zeros((rws, cls), dtype=np.uint8)
    
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
	states = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)


	
	arbtraj_list = []
	ytc_list = []
	tile_list = []
	# mask_list = []
	states_list = []
	traj_array_list = []


	for row in traj_list:
		traj_value = row[0]
		
		#year of conversion for either expansion or abandonmen
		ytx = row[1]
		##print 'ytx', ytx
		
		#year before conversion for either expansion or abandonment
		ybx = row[1]-1
		##print 'ybx', ybx

		#Return the indices of the pixels that have values of the ytc arbitrsy values of the traj.
		indices = (arr_traj == row[0]).nonzero()

		#stack indices so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))
        
        #get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:
			row = pixel_location[0] 
			col = pixel_location[1]



			##print '---------- new pixel to analyze -----------------------------------'
			#get the pixel value for ybx
			pixel_value_ybx =  cdls[ybx][row][col]
			#print 'pixel_value_ybx', pixel_value_ybx

			#get the pixel value for ytx
			pixel_value_ytx =  cdls[ytx][row][col]
			#print 'pixel_value_ytx', pixel_value_ytx 

			#### find the years stil before in the time series for this pixel location
			years_before_list = [i for i in data['global']['years'] if i < ytx]
			#print 'years_before_list', years_before_list
			list_before = []
			for year in years_before_list:
				list_before.append(cdls[year][row][col])
			
			# print 'list_before', list_before
			list_kernel = list_before[:-2]
			# print 'list_kernel', list_kernel
			# print 'len', len(list_kernel)

			#### find the alue of the years after cy!?!
			years_after_list = [i for i in data['global']['years'] if i > ytx]
			#print 'years_after_list', years_after_list
			list_after = [pixel_value_ytx]	
			for year in years_after_list:
				list_after.append(cdls[year][row][col])
			# print 'list_after', list_after


			list_entire=list_before+list_after

			tile_list.append(fc_count)
			arbtraj_list.append(traj_value)
			ytc_list.append(ytx)
			states_list.append(states[row][col])
			traj_array_list.append(str(list_entire))
			# print 'list_entire------', list_entire



			fuzzylist=[36,37,61,152,176]








   #          #### fruit and sod filter ##############################################################################################
			# if(np.isin(list_before, [59,66,68,69,71,74,77]).any() and np.isin(list_after, fuzzylist+[59,66,68,69,71,74,77]).all()):
			# 	# print 'fruit filter', list_entire
			# 	# outData[row,col] = data['refine']['arbitrary_crop']
			# 	# outData[row,col] = 201
			# 	mask_list.append(201)

            



			# ###### bad traj (general) ######################################################################################################
			# if((len(list_kernel) != 0) and (np.unique(np.isin(list_kernel, croplist)).all()) and (np.isin(list_after, croplist).all())):
			# 	# print 'bad traj (general)', list_entire
			# 	# outData[row,col] = data['refine']['arbitrary_crop']
			# 	# outData[row,col] = 202
			# 	mask_list.append(202)
		



			# ### bad traj-----> nd north V mask  ####################################
			# if traj_value in [300, 353, 482, 690]:
			# 	# print 'bad traj (nd-v)', list_entire
			# 	# outData[row,col] = data['refine']['arbitrary_crop']
			# 	# outData[row,col] = 203
			# 	mask_list.append(203)



			# ### rice----- ####################################
			# #####needs to have rice before AND after to be considered false conversion
			# if(np.isin(list_before, [3]).any()) and (np.isin(list_after, [3]).any()):
			# 	# print 'rice----', list_entire
			# 	# outData[row,col] = data['refine']['arbitrary_crop']
			# 	# outData[row,col] = 204
			# 	mask_list.append(204)






			

			# #####  create dev mask  ##################################################################################
			# ##### logic: dev never converted to crop 
			# ###Might need to refine this filter
			# if np.isin(list_before, [122,123,124]).any():
			# 	# print 'dev mask', list_entire
			# 	# outData[row,col] = data['refine']['arbitrary_noncrop']
			# 	# outData[row,col] = 101
			# 	mask_list.append(101)



			# #################  fuzzy filter #######################################################################
			# if(np.isin(list_entire, [36,37,61,152,176]).all()):
			# 	# print 'fuzzy filter', list_entire
			# 	# outData[row,col] = data['refine']['arbitrary_noncrop']
			# 	# outData[row,col] = 102
			# 	mask_list.append(102)






	##merge the lists into a tuple 
	# data_tuples = list(zip(arbtraj_list,states_list,ytc_list,tile_list,traj_array_list,mask_list))
	data_tuples = list(zip(tile_list, arbtraj_list,states_list,ytc_list,traj_array_list))
	# print 'data_tuples------------', data_tuples
	# print arbtraj_list
	# print ytc_list
	# print traj_array_list
	# print data_tuples
	df=pd.DataFrame(data_tuples, columns=['tile', 'traj', 'state', 'ytc', 'traj_array'])
	
	if df.empty:
		pass
	else:
		df.to_sql('tile_{}'.format(str(fc_count)), engine, schema='qaqc_tiles', if_exists='replace')
	

	tile_list = None
	arbtraj_list = None
	states_list=None
	ytc_list = None
	traj_array_list = None
	data_tuples = None
	df = None


    



def run(data):
	print "mask mutiple masks-----------+++++++++++++++++"
	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	for tile in tiles:
		os.remove(tile)

	traj_list = createReclassifyList(data)

	noncroplist = getNonCropList()

	croplist = getCropList()

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
	pool = Pool(processes=8)
	pool.map(execute_task, [(ed, data, traj_list, noncroplist, croplist) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data)


if __name__ == '__main__':
	run(data)
   
