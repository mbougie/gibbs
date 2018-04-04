import arcpy
from arcpy import env
from arcpy.sa import *
import pandas as pd
import numpy as np
import os
import glob
import sys
import psycopg2
from sqlalchemy import create_engine
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen




try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"





#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 




def execute_task(args):
	in_extentDict = args

	rg_combos = {'4w':["FOUR", "WITHIN"], '8w':["EIGHT", "WITHIN"], '4c':["FOUR", "CROSS"], '8c':["EIGHT", "CROSS"]}
	rg_instance = rg_combos['8w']


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)
    
	cond = "LINK <> 3"
	print 'cond: ',cond

	raster_in = Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s18\\core\\core_s18.gdb\\s18_v4_traj_cdl30_b_2008to2017_rfnd_v2_n8h_mtr_8w_mmu5')
	raster_rg = RegionGroup(raster_in, rg_instance[0], rg_instance[1], "ADD_LINK")
	# raster_oid = Lookup(raster_rg, "OID")
	# raster_mask = SetNull(raster_rg, raster_rg, cond)


	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")

	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	# outname2 = "tile_rg2_" + str(fc_count) +'.tif'
	# outpath2 = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname2)

	# raster_shrink.save(outpath)
	raster_rg.save(outpath)


	arcpy.AddField_management(Raster(outpath), field_name='tile', field_type='TEXT')
	expression = "{}".format(str(fc_count))
	arcpy.CalculateField_management(outpath, "tile", expression, "PYTHON_9.3")

	# arcpy.CopyRaster_management(outpath, outpath2,"DEFAULTS","","","","","16_BIT_UNSIGNED")








def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	for tile in tilelist:

		print tile
		yo = tile.split('_')
		hi = yo[2].split('.')
		print hi[0]

		arcpy.BuildRasterAttributeTable_management(Raster(tile))

		arcpy.AddField_management(Raster(tile), field_name='tile', field_type='TEXT')
		arcpy.AddField_management(Raster(tile), field_name='obj_id', field_type='TEXT')

		expression = "{}".format(hi[0])
		arcpy.CalculateField_management(tile, "tile", expression, "PYTHON_9.3")

		# arcpy.CalculateField_management(tile, "obj_id",  "!tile! + '_' + str(!OID!)", "PYTHON_9.3")





def mosiacRasters_v2():
	# set the engine.....
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	querylist=[]

	for tile in tilelist:

		print tile
		tile_split=tile.split("\\")
		table_name=tile_split[1].split(".")

		# Execute AddField twice for two new fields
		fields = [f.name for f in arcpy.ListFields(tile)]

		# converts a table to NumPy structured array.
		arr = arcpy.da.TableToNumPyArray(tile,fields)
		print arr

		# convert numpy array to pandas dataframe
		df = pd.DataFrame(data=arr)

		df.columns = map(str.lower, df.columns)

		print 'df-----------------------', df

		mtr3 = df['link'] == 3 
		mtr4 = df['link'] == 4 
        
		yo = df[mtr3 | mtr4]

		print 'df------- subset ---', yo
		# # # use pandas method to import table into psotgres
		yo.to_sql(table_name[0], engine, schema='eric')

		querylist.append("SELECT * FROM eric.{}".format(table_name[0]))



	query = "CREATE TABLE eric.merged_table AS " + ' UNION '.join(querylist) + " ORDER BY tile; ALTER TABLE eric.merged_table ADD COLUMN id SERIAL PRIMARY KEY;"
	print '--- query ----', query
    
	cur = conn.cursor()
	cur.execute(query);

	conn.commit()
	print "Records created successfully";
	conn.close()






def reclassTiles(field, mtr):
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	for tile in tilelist:

		print tile
		tile_split=tile.split("\\")
		output = '{}/mtr{}/{}'.format(tile_split[0], mtr, tile_split[1].replace(".tif", "_mtr{}_{}.tif".format(mtr, field)))
		print output

		table_name=tile_split[1].split(".")
		tile_num = table_name[0].split("_")

		return_string = getReclassifyValuesString(tile_num[1], field, mtr)
		arcpy.gp.Reclassify_sa(tile, "Value", return_string, output, "NODATA")


	mosiacRasters(mtr, field)






def getReclassifyValuesString(tile_num, field, mtr):
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
    
    query = "SELECT value, {} FROM eric.merged_table WHERE link={} AND tile='{}'".format(field, mtr, tile_num)
    #DDL: add column to hold arrays
    cur.execute(query);
    
    #create empty list
    reclassifylist=[['0 NODATA']]

    # fetch all rows from table
    rows = cur.fetchall()
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        ww = [str(row[0]) + ' ' + str(int(row[1]))]
        reclassifylist.append(ww)
    
    #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
    columnList = ';'.join(sum(reclassifylist, []))
    # print columnList
    
    #return list to reclassifyRaster() fct
    return columnList






def mosiacRasters(mtr, field):
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/mtr{}/*{}.tif".format(mtr, field))
	print tilelist 
	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(tilelist[0])

	gdb = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s18\\core\\core_s18.gdb'
	filename = 's18_mtr{}_{}'.format(mtr, field)
	path = '{}\\{}'.format(gdb, filename)

	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, "32_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	# #Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(path, "Overwrite")

	# # Overwrite pyramids
	gen.buildPyramids(path)







if __name__ == '__main__':

	# #####  remove a files in tiles directory
	# tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
	# for tile in tiles:
	# 	os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_mtr', ["oid","SHAPE@"]):
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

	#######create a process and pass dictionary of extent to execute task
	# pool = Pool(processes=5)
	# # pool = Pool(processes=cpu_count())
	# pool.map(execute_task, extDict.items())
	# pool.close()
	# pool.join

	print('---starting mosaic process----')
	# mosiacRasters_v2()
    
	reclassTiles('id', '3')
	# reclassTiles('count', '3')


	# reclassTiles('id', '4')
	reclassTiles('count', '4')







