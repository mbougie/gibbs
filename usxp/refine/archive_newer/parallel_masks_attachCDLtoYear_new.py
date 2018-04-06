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



arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



###make this a general function
def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\current_instance.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



def createReclassifyList():
	cur = conn.cursor()

	query = "SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc = 2011".format(data['pre']['traj']['filename'], data['core']['lookup'])
	# query = """ SELECT 
	# 			  traj_try.traj_rfnd, 
	# 			  traj_try.state, 
	# 			  traj_try."Value" as traj_rfnd_state, 
	# 			  v4_traj_lookup_2008to2017_v3.ytc
	# 			FROM 
	# 			  refinement_new.traj_try, 
	# 			  pre.v4_traj_cdl30_b_2008to2017, 
	# 			  pre.v4_traj_lookup_2008to2017_v3
	# 			WHERE 
	# 			  v4_traj_cdl30_b_2008to2017."Value" = traj_try.traj_rfnd AND
	# 			  v4_traj_lookup_2008to2017_v3.traj_array = v4_traj_cdl30_b_2008to2017.traj_array AND ytc IS NOT NULL
	#         """




# create table refinement_new_tiles_union.s19_2011_full as 
# SELECT 
# traj_state.traj_rfnd, 
# traj_state.state, 
# traj_state."Value" as traj_rfnd_state, 
# v4_traj_lookup_2008to2017_v3.ytc as year,
# s19_2011.traj_array
# FROM 
# refinement_new.traj_state, 
# pre.v4_traj_cdl30_b_2008to2017, 
# pre.v4_traj_lookup_2008to2017_v3,
# refinement_new_tiles_union.s19_2011
# WHERE 
# v4_traj_cdl30_b_2008to2017."Value" = traj_state.traj_rfnd AND
# v4_traj_lookup_2008to2017_v3.traj_array = v4_traj_cdl30_b_2008to2017.traj_array AND
# traj_state.traj_rfnd = s19_2011.traj AND ytc IS NOT NULL







	print 'query:', query

	cur.execute(query)
	#create empty list
	fulllist=[[0,0,"NODATA"]]

	# fetch all rows from table
	rows = cur.fetchall()
	print rows
	print 'number of records in lookup table', len(rows)
	return rows
	



##create global objects to reference through the script

data = getJSONfile()
# print data
traj_list = createReclassifyList()




def execute_task(in_extentDict):

	stco_atlas = "tile_"+str(in_extentDict[0])
	
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

	cls = 3577
	rws = 13789


	# outData = numpy.zeros((rows,cols), numpy.int16)
	outData = np.zeros((rws, cls), dtype=np.int)
    
    ### create numpy arrays for input datasets cdls, traj and traj_state
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
	
	arr_traj = arcpy.RasterToNumPyArray(in_raster=data['pre']['traj_rfnd']['path'], lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
    
	traj_state = arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\conf.gdb\\traj_state', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	

	# find the location of each pixel labeled with specific arbitray value in the rows list  
	mainlist = []
	for traj in traj_list:
		#Return the indices of the pixels that have values of the ytc arbitrary values of the traj.
		indices = (arr_traj == traj[0]).nonzero()

		#stack indices so easier to work with
		stacked_indices=np.column_stack((indices[0],indices[1]))
        
        #get the x and y location of each pixel that has been selected from above
		for pixel_location in stacked_indices:
			row = pixel_location[0] 
			col = pixel_location[1]

			#####define elements
			trajectory=traj[0]
			year=traj[1]
			state=str(traj_state[row][col])
			tile=str(in_extentDict[0])

			templist = [trajectory,year,state,tile,row,col]
			for year in data['global']['years']:
				templist.append(cdls[year][row][col])
            
			mainlist.append(templist)
	

	arcpy.ClearEnvironment("extent")
    
	if len(mainlist) > 0:

		to_string = map(str, data['global']['years'])
		year_columns = ["cdl_" + to_string for to_string in to_string]

		df = pd.DataFrame(mainlist, columns=['traj','year','state','tile','rows','cols'] + year_columns)

		df.to_sql(stco_atlas, engine, schema='refinement_tiles_2011')

		addTrajArrayField('refinement_tiles_2011', stco_atlas, year_columns)


def addTrajArrayField(schema, tablename, fields):
    #this is a sub function for addGDBTable2postgres()
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields)
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE {}.{} ADD COLUMN traj_array integer[];'.format(schema, tablename));
    
    #DML: insert values into new array column
    cur.execute('UPDATE {}.{} SET traj_array = ARRAY[{}];'.format(schema, tablename, columnList));
    
    conn.commit()
    print "Records created successfully";
    # conn.close()







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

	# for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_refine'], ["SHAPE@"]):
	# 	extent_curr = row[0].extent
	# 	ls = []
	# 	ls.append(extent_curr.XMin)
	# 	ls.append(extent_curr.YMin)
	# 	ls.append(extent_curr.XMax)
	# 	ls.append(extent_curr.YMax)
	# 	extDict[count] = ls
	# 	count+=1

	for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['fishnet_refine'], ["OID@","SHAPE@"]):
		oid = row[0]
		print 'oid:',oid
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[oid] = ls

    
	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=6)
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join

	# mosiacRasters()


# run()






######## temp sql  ###############################################################



# UPDATE refinement_union.traj_state
# SET state=subquery.mod_state
# FROM (SELECT '0' || state as mod_state, state
# FROM  refinement_union.traj_state where length(state) = 1) AS subquery
# WHERE traj_state.state=subquery.state  








# create table refinement_union.traj_state_t2 as 
# SELECT 
#   main.index, 
#   main.objectid, 
#   main.value, 
#   main.count, 
#   main.traj_rfnd, 
#   main.state, 
#   main.acres, 
#   states.atlas_st, 
#   states.st_abbrev,
#   count/(select sum(count) from refinement_union.traj_state where ytc is not null group by state having main.state = state) * 100 as percent
  
# FROM 
#   refinement_union.traj_state as main,
#   pre.v4_traj_lookup_2008to2017_v3,
#   pre.v4_traj_cdl30_b_2008to2017,
#   spatial.states
# WHERE 
#   states.atlas_st = main.state 
#   AND v4_traj_cdl30_b_2008to2017."Value" = main.traj_rfnd 
#   AND v4_traj_cdl30_b_2008to2017.traj_array = v4_traj_lookup_2008to2017_v3.traj_array
#   AND ytc is not null
   
