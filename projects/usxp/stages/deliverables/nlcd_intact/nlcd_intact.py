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





def addGDBTable2postgres_table(gdb, pgdb, schema, table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))
	arcpy.env.workspace = gdb

	fields = [f.name for f in arcpy.ListFields(table)]
	# print fields


	arr = arcpy.da.TableToNumPyArray(table, fields)
	# print arr


	#### convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)
	### remove column
	del df['OBJECTID']
	print df

	# ##perform a psuedo pivot table
	df=pd.melt(df, id_vars=["LABEL"],var_name="value", value_name="count")

	df.columns = map(str.lower, df.columns)

	print df

	#### format column in df #########################
	## strip character string off all cells in column
	df['value'] = df['Value'].map(lambda x: x.strip('value_'))

	print df

	# df.to_sql(table, engine, schema=schema)





# def addGDBTable2postgres_state(pgdb, schema, currentobject):
#     print 'addGDBTable2postgres_histo..................................................'
#     print currentobject

#     ##set the engine.....
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

#     # Execute AddField twice for two new fields
#     fields = [f.name for f in arcpy.ListFields(currentobject)]

#     # converts a table to NumPy structured array.
#     arr = arcpy.da.TableToNumPyArray(currentobject,fields)
#     print arr


#     #### convert numpy array to pandas dataframe
#     df = pd.DataFrame(data=arr)
#     ### remove column
#     del df['OBJECTID']
#     print df

#     # ##perform a psuedo pivot table
#     # df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


#     df.columns = map(str.lower, df.columns)

#     print df
    
#     # #### format column in df #########################
#     # ## strip character string off all cells in column
#     # df['atlas_st'] = df['atlas_st'].map(lambda x: x.strip('atlas_'))
#     # ## remove comma from year
#     # df['value'] = df['label'].str.replace(',', '')

#     # print df


#     # print 'pixel conversion:', getPixelConversion2Acres(30)

#     # ####add column 
#     # df['acres'] = df['count']*getPixelConversion2Acres(30)

#     tablename = currentobject.split('\\')[-1]
#     print 'tablename', tablename

#     print df

#     df.to_sql(tablename, engine, schema=schema)

#     # MergeWithGeom(df, tablename, eu, eu_col)




gdb='I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\nlcd_intact_t2.gdb'
pgdb='usxp_deliverables'
schema='nlcd_intact'
table='combine_intact_visualization_raw_b_hist'



addGDBTable2postgres_table(gdb=gdb, pgdb= pgdb, schema=schema, table=table)