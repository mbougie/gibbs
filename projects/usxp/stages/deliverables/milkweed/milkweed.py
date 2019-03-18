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


#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



# def addGDBTable2postgres(gdb, pg_db, schema, table):
# 	print("addGDBTable2postgres().............")
# 	# set the engine.....
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pg_db))

# 	input_table = '{}\\{}'.format(gdb, table)
# 	print input_table

# 	# Execute AddField twice for two new fields
# 	fields = [f.name for f in arcpy.ListFields(input_table)]

# 	# converts a table to NumPy structured array.
# 	arr = arcpy.da.TableToNumPyArray(input_table,fields)
# 	print arr

# 	# # convert numpy array to pandas dataframe
# 	df = pd.DataFrame(data=arr)

# 	# df[fields[1]] = df[fields[1]].map({'No': 0, 'Yes': 1, 'Unranked':2, 'null':255})

# 	# df = df.groupby(['mukey'], sort=False)['comppct_r'].max()

# 	df.columns = map(str.lower, df.columns)
# 	print 'df-----------------------', df

# 	df.to_sql(table, con=engine, schema=schema)



def createReclassifyList_v1(schema, table, fieldlist):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query = " SELECT {2},{3} from {0}.{1} WHERE {3} IS NOT NULL".format(schema, table, fieldlist[0], fieldlist[1])
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    fulllist.append(templist)
	return fulllist




def createReclassifyList_v2():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')
	query='SELECT objectid,stem_acre FROM main.milkweed'
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    fulllist.append(templist)
	return fulllist



def  reclassRaster(schema, inraster, fieldlist):
	gdb = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(schema)
	arcpy.env.workspace = gdb

	outraster = Reclassify(inraster, "Value", RemapRange(createReclassifyList_v1(schema, inraster, fieldlist)), "NODATA")

	outpath = "{}_rc".format(inraster)
	outraster.save(outpath)



def createMilkweedRaster(schema, inraster):
	gdb = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(schema)
	arcpy.env.workspace = gdb
	county_raster = Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\county_30m')

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')

	#####raster 1 is concerned with county value so convert the value of the s35_traj_bfc_fc_rc to the county value where grouped 
	raster_1 = SetNull(inraster, county_raster, "VALUE <> 1")
	county_raster=None

	###reclass the pixels with the stems_acres coumn by county value from the rasster above
	reclassed_1 = Reclassify(raster_1, "Value", RemapRange(createReclassifyList_v2()), "NODATA")
	raster_1=None

	########change ALL pixels where s35_traj_bfc_fc_rc = 2 to value 61.37.  Not concerned with county spatial info.
	raster_2 = SetNull(inraster, 61.37, "VALUE <> 2")

	filename = "s35_{}".format(schema)

	#### mosiac raster_1 and raster_2 to get the milkweed dataset
	arcpy.MosaicToNewRaster_management([reclassed_1, raster_2], gdb, filename, inTraj.spatialReference, "8_BIT_UNSIGNED", 30, "1", "LAST","FIRST")





def blockStats(schema, inraster):
	####SUM up the pixels values per block to get total stems for the determined acres from the block
	arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(schema)
	nbr = NbrRectangle(100, 100, "CELL")
	outBlockStat = BlockStatistics(inraster, nbr, "SUM", "DATA")
	inraster=None
	outAggreg = Aggregate(outBlockStat, 100, "MAXIMUM", "TRUNCATE", "DATA")
	outBlockStat=None

	outpath = "{}_bs3km".format(schema)
	outAggreg.save(outpath)
	outAggreg=None



def addGDBTable2postgres_histo(gdb, tablename, eu_col):
    print 'addGDBTable2postgres_histo..................................................'
    print tablename

    arcpy.env.workspace = "D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb".format(gdb)

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(tablename)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(tablename,fields)
    print arr

    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name=eu_col, value_name="count")


    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['state'] = df[eu_col].map(lambda x: x.strip('atlas_'))
    ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    print df

    print 'lkl', gen.getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*gen.getPixelConversion2Acres(30)


    print df

    df.to_sql(tablename, engine, schema=gdb)









if __name__ == '__main__':
	print ('this is the main function')
	#####create s35_traj_bfc_fc trajectory dataset using the conmbine funtion

	#####reclass the raster with grouped binary value 1 or 2
	# reclassRaster(schema='milkweed', inraster='s35_traj_bfc_fc', fieldlist=['value', 'grouped'])
	
	#####reclass raster with the stems_acre value from the main.milkweed dataset in the nri database derived with the NRI code in nri_main.py script
	# createMilkweedRaster(schema='milkweed', inraster='s35_traj_bfc_fc_rc')
	
	#####get sum of stems in the block
	# blockStats(schema='milkweed', inraster='s35_milkweed')



	# addGDBTable2postgres_histo(gdb='milkweed', tablename='s35_milkweed_state_hist', eu_col='altas_')


	createReclassifyList_v2()


