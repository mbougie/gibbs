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



def addGDBTable2postgres(gdb, pg_db, schema, tablename, fields):
	print("addGDBTable2postgres().............")
	# set the engine.....
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pg_db))

	input_table = '{}\\{}'.format(gdb, tablename)
	print input_table


	##### sub-optimal solution  --- need to create a function to chenck if null value is a unique value!!!
	# null_value='null'
	# null_value=255
	# Execute AddField twice for two new fields
	# fields = [f.name for f in arcpy.ListFields(input_table)]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(input_table,fields)
	print arr


	# # convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)


	# df['crosswalked'] = df['hydricrating'].map({'No': 0, 'Yes': 1, 'Unranked':2, 'null':255})

	df.columns = map(str.lower, df.columns)
	print 'df-----------------------', df

	df.to_sql(tablename, con=engine, schema=schema)







def createReclassifyList(schema, table, fieldlist):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query = " SELECT {2} from {0}.{1} WHERE {1} IS NOT NULL".format(schema, table, ','.join(fieldlist))
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






def mosaicRasters(map_project, tablename):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("D:/projects/usxp/deliverables/maps/{}/tiles/*.tif".format(map_project))
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')

	filename = 'gssurgo_{}_30m'.format(tablename)
	print 'filename:', filename

	gdb = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(map_project)

	####suboptimal need to detrmine the appropraite datatype with a function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	datatype="16_BIT_UNSIGNED"
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, datatype, 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management('{0}\\{1}'.format(gdb, filename), "Overwrite")

	# Overwrite pyramids
	# gen.buildPyramids('{0}\\{1}'.format(gdb, filename))







def run(schema, tablename, fields):

	print ('------------------------------------------------this is the run function------------------------------------------------------------------')
	####add the table to postgres
	addGDBTable2postgres(gdb='D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb', pg_db='usxp_deliverables', schema=schema, tablename=tablename, fields=fields)

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

	# mosaicRasters(schema, table)



if __name__ == '__main__':
	print ('this is the main function')
	###!!!!!!!!!!!!!!!!NOTE addGDBTable2postgres() function needs work!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	###!!!!!!!!!!!!!!!!NOTE mosaicRasters() function needs work!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

	#####arguments for suitability dataset ###################################
	run(schema='hydric', tablename='muaggatt', fields=['mukey', 'hydclprs'])
	
	#####arguments for slope dataset ###################################
	# run(schema='hydric', table='component', fieldlist=['mukey', 'cokey', '', ])


	#####arguments for hydric dataset ###################################
	# addGDBTable2postgres(gdb='D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb', pg_db='usxp_deliverables', schema='hydric', table='component', fields=['mukey', 'cokey', 'hydricrating'])
	# addGDBTable2postgres(gdb='D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb', pg_db='usxp_deliverables', schema='hydric', table='cohydriccriteria', fields=['cokey','hydriccriterion'])
	# run(schema='hydric', table='cohydriccriteria', fieldlist=['cohydcritkey','hydriccriterion'])



