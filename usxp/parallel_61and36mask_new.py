# Import system modules
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
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
import gdal



arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



gdal.AllRegister()

# open the image-----suboptimal ----create this without a file
inDs = gdal.Open("C:/Users/Bougie/Desktop/Gibbs/temp/test_temp.tif")


# create the output image
# driver = inDs.GetDriver()
driver = gdal.GetDriverByName('GTiff')


###make this a general function
def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template


data = getJSONfile()
print data

###NOTE STILL HAVE TO DEAL WITH YFC IN QUERY BELOW  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def createReclassifyList():
	cur = conn.cursor()

	query = 'SELECT "Value",ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL'.format(data['pre']['traj']['filename'], data['pre']['traj']['lookup'])
	print 'query:', query

	cur.execute(query)
	#create empty list
	fulllist=[[0,0,"NODATA"]]

	# fetch all rows from table
	rows = cur.fetchall()
	print rows
	print 'number of records in lookup table', len(rows)
	return rows
	

getthelist = createReclassifyList()



def execute_task(in_extentDict):

	fc_count = in_extentDict[0]
	print 'fc_count-------------------------------------', fc_count

	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	#The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	# arcpy.env.snapRaster = nibble.inYTC
	# arcpy.env.cellsize = nibble.inYTC
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	cls = 21973
	rws = 13789


	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((13789, 21973), dtype=np.int)
    
    ### create numpy arrays for input datasets cdls and traj
	cdls = {
			2008:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2008', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2009:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2009', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2010:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2010', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2011:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2011', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2012:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2012', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2013:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2013', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2014:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2014', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2015:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2015', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973),
			2016:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2016', lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)
	       }
	
	arr_traj = arcpy.RasterToNumPyArray(in_raster='\\'.join([data['pre']['traj']['gdb'],data['pre']['traj']['filename']]), lower_left_corner = arcpy.Point(XMin,YMin), nrows = 13789, ncols = 21973)

    #suboptimal ----create this dynamically
	years = np.array([2008,2009,2010,2011,2012])

	# find the location of each pixel labeled with specific arbitray value in the rows list  
	for row in getthelist:
		#year of conversion for either expansion or abandonment
		ytx = row[1]
		#year before conversion for either expansion or abandonment
		ybx = row[1]-1

		#Return the indices of the pixels that have values of the ytc arbitrsy values of the traj.
		indices = (arr_traj == row[0]).nonzero()

		#stack indices so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))
        
        #get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:
			row = pixel_location[0] 
			col = pixel_location[1]
            
            #get the pixel value for ytx
			pixel_value_ytx =  cdls[ytx][row][col]
			#get the pixel value for ybx
			pixel_value_ybx =  cdls[ybx][row][col]

			#####  create dev mask componet
			if pixel_value_ybx in [122,123,124]:
				outData[row,col] = 251

	        #####  create 36_61 mask componet
			if pixel_value_ytx in [36,61]:
				#find the years stil left in the time series for this pixel location
				yearsleft = [i for i in years if i > ytx]
	
                #only focus on the extended series ---dont care about 2012
				if len(yearsleft) > 1:
					#create templist to hold the rest of the cld values for the time series.  initiaite it with the first cdl value
					templist = [pixel_value_ytx]
					for year in yearsleft:
						# print 'year', year
						# print 'cdls[year][row][col] :', cdls[year][row][col]
						templist.append(cdls[year][row][col])

					#check if all elements in array are the same
					if len(set(templist)) == 1:
						outData[row,col] = 252

		




	arcpy.ClearEnvironment("extent")

	# print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	# #create
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
	# arcpy.env.workspace =  data['refine']['gdb']
	

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inYTC=Raster(nibble.inYTC)
	inTraj=Raster(nibble.inTraj)

	arcpy.env.extent = inYTC.extent
	arcpy.env.snapRaster = inYTC
	arcpy.env.cellsize = inYTC
	arcpy.env.outputCoordinateSystem = inYTC

	# mosaic = 'traj_ytc30_2008to2015_mask'

	masks_gdb = defineGDBpath(data['refine']['gdb'])

	##sub-optimal need to create this temp dataset or and then copy are remove the dataset otherwise are not alligned
	out_name = nibble.inTraj_name+'_msk36and61_temp'

	outpath = masks_gdb+out_name


	##### CreateRasterDataset_management (out_path, out_name, cellsize=30, pixel_type, raster_spatial_reference, number_of_bands)
	arcpy.CreateRasterDataset_management(masks_gdb, out_name, 30, "8_BIT_UNSIGNED", inTraj.spatialReference, 1, "", "", "", "", "")

	##### Mosaic_management (inputs, target, {mosaic_type}, {colormap}, {background_value}, {nodata_value}, {onebit_to_eightbit}, {mosaicking_tolerance}, {MatchingMethod})
	# arcpy.Mosaic_management(tilelist, outpath, "", "", "", 0, "", "", "")

	##### copy raster so it "snaps" to the other datasets -------suboptimal
	##### CopyRaster_management (in_raster, out_rasterdataset, {config_keyword}, {background_value}, {nodata_value}, {onebit_to_eightbit}, {colormap_to_RGB}, {pixel_type}, {scale_pixel_value}, {RGB_to_Colormap}, {format}, {transform})
	# arcpy.CopyRaster_management(Raster(outpath), nibble.inTraj_name+'_msk36and61')
	arcpy.CopyRaster_management(outpath, nibble.inTraj_name+'_msk_new', "", "", "256", "NONE", "NONE", "", "NONE", "NONE", "", "NONE")

	##### delete the initial raster
	# arcpy.Delete_management(outpath)




    





# def run():  
if __name__ == '__main__':
	#instantiate the class inside run() function
	# nibble = ProcessingObject(series, res, mmu, years, name)

	tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_mtr'], ["SHAPE@"]):
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
	# pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	# pool.map(execute_task, [(ed, nibble) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(nibble)


# run()
    
   
