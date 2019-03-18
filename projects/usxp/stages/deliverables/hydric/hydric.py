import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import webcolors as wc
import palettable
# import matplotlib as mpl

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
# import replace_61_w_hard_crop


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def addGDBTable2postgres_hydric(gdb, pg_db, schema, table, fields):
	print("addGDBTable2postgres().............")
	# set the engine.....
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pg_db))

	input_table = '{}\\{}'.format(gdb, table)
	print input_table

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(input_table,fields, null_value='null')
	print arr

	# # convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)

	df[fields[1]] = df[fields[1]].map({'No': 0, 'Yes': 1, 'Unranked':2, 'null':255})

	# df = df.groupby(['mukey'], sort=False)['comppct_r'].max()

	df.columns = map(str.lower, df.columns)
	print 'df-----------------------', df




	df.to_sql(table, con=engine, schema=schema)







def createReclassifyList(schema, table, field):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query = " SELECT mukey, {2} from {0}.hydric_final".format(schema, table, field[0])
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    fulllist.append(templist)
	return fulllist


  
# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, reclass_list, in_raster, schema = args

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	cdl30_2017=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')
	arcpy.env.snapRaster = cdl30_2017
	# arcpy.env.cellsize = 30
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	raster_reclassed = Reclassify(Raster(in_raster), "Value", RemapRange(reclass_list), "NODATA")
	nbr = NbrRectangle(3, 3, "CELL")
	outBlockStat = BlockStatistics(raster_reclassed, nbr, "MAJORITY", "DATA")
	raster_reclassed=None
	outAggreg = Aggregate(outBlockStat, 3, "MAXIMUM", "TRUNCATE", "DATA")
	outBlockStat=None


	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("D:/projects/usxp/deliverables/maps/{}/".format(schema), r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	outAggreg.save(outpath)
	outAggreg=None

	outpath=None






def mosiacRasters(map_project, tablename):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("D:/projects/usxp/deliverables/maps/{}/tiles/*.tif".format(map_project))
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')

	filename = 'gssurgo_{}_30m'.format(tablename)
	print 'filename:', filename

	gdb = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(map_project)
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management('{0}\\{1}'.format(gdb, filename), "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids('{0}\\{1}'.format(gdb, filename))



def setNull_yo(map_project, tablename, newtable):
	arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(map_project)

	filename = 'gssurgo_{}_30m'.format(tablename)
	print 'filename:', filename

	cond = "Value <> 3" 
	outraster = SetNull('D:\\projects\\usxp\\deliverables\\s35\\s35.gdb\\s35_mtr', filename, cond)

	outraster.save(newtable)

	gen.buildPyramids(newtable)




def blockStats(map_project, tablename, cellsize):
	arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(map_project)
	
	nbr = NbrRectangle(cellsize, cellsize, "CELL")
	outBlockStat = BlockStatistics(tablename, nbr, "MAJORITY", "DATA")
	outAggreg = Aggregate(outBlockStat, cellsize, "MAXIMUM", "TRUNCATE", "DATA")
	outBlockStat=None

	output_raster = "{}_bs3km".format(tablename)
	outAggreg.save(output_raster)
	gen.buildPyramids(output_raster)


# def addGDBTable2postgres():
#     # set the engine.....
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
    
#     # # path to the table you want to import into postgres
#     # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'
#     raster = 'D:\\projects\\usxp\\deliverables\\maps\\suitability\\suitability.gdb\\suitability'

#     # Execute AddField twice for two new fields
#     fields = [f.name for f in arcpy.ListFields(raster)]
   
#     # converts a table to NumPy structured array.
#     arr = arcpy.da.TableToNumPyArray(raster,fields)
#     print arr
    
#     # convert numpy array to pandas dataframe
#     df = pd.DataFrame(data=arr)

#     df.columns = map(str.lower, df.columns)

#     print 'df-----------------------', df

#     schema = 'suitability'
#     tablename = 'suitability'


#     # df[].sum(axis=1)
    
#     # # # # use pandas method to import table into psotgres
#     # df.to_sql(tablename, engine, schema=schema)
    
#     # # # #add trajectory field to table
#     # addAcresField(schema, tablename, 30)




# def addAcresField(schema, tablename, res):

# 	try:
# 		conn = psycopg2.connect("dbname= 'usxp_deliverables' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
# 	except:
# 		print "I am unable to connect to the database"
# 	#this is a sub function for addGDBTable2postgres()

# 	cur = conn.cursor()

# 	####DDL: add column to hold arrays
# 	query = 'ALTER TABLE {}.{} ADD COLUMN acres bigint'.format(schema, tablename)
# 	print query
# 	cur.execute(query)

# 	#####DML: insert values into new array column
# 	cur.execute("UPDATE {0}.{1} SET acres=count*{2}".format(schema, tablename, gen.getPixelConversion2Acres(res) ))
# 	conn.commit() 






def run(schema, table, fieldlist, newtable):

	print ('------------------------------------------------this is the run function------------------------------------------------------------------')
	####add the table to postgres
	# addGDBTable2postgres_hydric(gdb='D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb', pg_db='usxp_deliverables', schema=schema, table=table, fields=['mukey']+fieldlist)

	# tiles = glob.glob("D:/projects/usxp/deliverables/maps/{}/tiles/*".format(schema))
	# for tile in tiles:
	# 	os.remove(tile)

	# reclass_list = createReclassifyList(schema, table, fieldlist)
	# in_raster = 'D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb\\MapunitRaster_conus_10m'

	# fishnet = 'fishnet_cdl_49_7'

	# #get extents of individual features and add it to a dictionary
	# extDict = {}

	# for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["oid","SHAPE@"]):
	# 	atlas_stco = row[0]
	# 	print atlas_stco
	# 	extent_curr = row[1].extent
	# 	ls = []
	# 	ls.append(extent_curr.XMin)
	# 	ls.append(extent_curr.YMin)
	# 	ls.append(extent_curr.XMax)
	# 	ls.append(extent_curr.YMax)
	# 	extDict[atlas_stco] = ls

	# print 'extDict', extDict
	# print'extDict.items',  extDict.items()


	# #######create a process and pass dictionary of extent to execute task
	# pool = Pool(processes=4)
	# pool.map(execute_task, [(ed, reclass_list, in_raster, schema) for ed in extDict.items()])
	# pool.close()
	# pool.join

	# mosiacRasters(schema, table)


	# ## setnull function to reclass mtr3 with nicc values!!!
	setNull_yo(schema, table, newtable)


	# 	### setblockstats!!!
	blockStats(schema, newtable, 100)






	#### might not need anymore!! ###############################
	### export raster attribute table to postgres
	# addGDBTable2postgres()



if __name__ == '__main__':
	print ('this is the main function')
	run(schema='hydric', table='component', fieldlist=['hydricrating', 'comppct_r'], newtable='s35_hydric')



