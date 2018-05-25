
from sqlalchemy import create_engine
import numpy as np, sys, os
import fnmatch
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import shutil
import matplotlib.pyplot as plt
#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


# set the engine.....
engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')


# class fireItUp():
# 	def __init__(self):
# 	    self.dbname='usxp'
# 	    self.user='mbougie'
# 	    self.host='144.92.235.105'
# 	    self.password='Mend0ta!'
	
# 	def getConn(self):
# 		try:
# 			conn = psycopg2.connect("dbname={} user={} host={} password={}".format(self.dbname, self.user, self.host, self.password))
# 			return conn
# 		except:
# 			print "I am unable to connect to the database"


# 	def getEngine(self):
# 		engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
# 		return engine






# # class getItDone():

# conn = fireItUp().getConn()
# engine = fireItUp().getEngine()

# cur = conn.cursor()

# print cur
# print engine






def addGDBTable2postgres(data,yxc,schema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(data['post'][yxc]['path'])]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(data['post'][yxc]['path'],fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df
    
    # # # use pandas method to import table into psotgres
    df.to_sql(data['post'][yxc]['filename'], engine, schema='counts_yxc')
    
    # #add trajectory field to table
    addAcresField('counts_yxc', data['post'][yxc]['filename'], yxc, '30')









def addAcresField(schema, tablename, yxc, res):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    ####DDL: add column to hold arrays
    ddl_query = 'ALTER TABLE {}.{} ADD COLUMN acres bigint, ADD COLUMN series text, ADD COLUMN yxc text, ADD COLUMN series_order integer'.format(schema, tablename)
    print ddl_query
    cur.execute(ddl_query)


    #####DML: insert values into new array column
    dml_query="UPDATE {0}.{1} SET acres=count*{2}, series='{3}', yxc='{4}', series_order={5}".format(schema, tablename, gen.getPixelConversion2Acres(res), tablename.split("_")[0], yxc, int(tablename.split("_")[0][1:]))
    print dml_query
    cur.execute(dml_query)
    
    conn.commit() 




def createMergedTable(schema):
  cur = conn.cursor()
  query="SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND SUBSTR(table_name, 1, 1) = 's';".format(schema)
  cur.execute(query)
  rows = cur.fetchall()
  print rows
  
  table_list = []
  for row in rows:
    query_temp="SELECT value as years,count,acres,series,yxc,series_order FROM counts_yxc.{}".format(row[0])
    table_list.append(query_temp)

  query_final = "DROP TABLE IF EXISTS counts_yxc.merged_series; CREATE TABLE counts_yxc.merged_series AS {}".format(' UNION '.join(table_list))
  print query_final
  cur.execute(query_final)
  conn.commit()





# def zonalHist(env_path, wc):
# 	# set the engine.....
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')


# 	in_zone_data = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states'
# 	zone_field = 'Value'

# 	# Set environment settings
# 	env.workspace = env_path

# 	df_list=[]

# 	rasters = arcpy.ListRasters("*{}*".format(wc), "GRID")
# 	for in_value_raster in rasters:
# 		print(in_value_raster)

# 		hist_table='{}_hist'.format(in_value_raster)
# 		print(hist_table)

# 		#ZonalHistogram(in_zone_data, zone_field, in_value_raster, hist_table)

# 		df_current=changeTableFormat(hist_table)

# 		df_list.append(df_current)

# 	###merge all the seperate dataframes into one.
# 	df_final=pd.concat(df_list)

# 	print 'df_final:', df_final

# 	# df_final.to_sql('s20_ytc30_2008to2017_mmu5_fc_states', engine, schema='counts_yxc')





def changeTableFormat(data, yxc, year, table):
	print('inside new', table)

	# arr = arcpy.da.TableToNumPyArray(table, '*')

	fields = [f.name for f in arcpy.ListFields(table)]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(table,fields)
	# print arr

	# convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)
	#r## remove column
	del df['OBJECTID']
	print df

	##perform a psuedo pivot table
	df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


	df.columns = map(str.lower, df.columns)
    
    #### format column in df #########################
	## strip character string off all cells in column
	df['atlas_st'] = df['atlas_st'].map(lambda x: x.strip('Value_'))
	## add zero infront of string if length is less than 2
	df['atlas_st'] = df['atlas_st'].apply(lambda x: '{0:0>2}'.format(x))

	#### add columns to table ########################
	df['series'] = data['global']['instance']
	df['yxc'] = yxc
	df['acres'] = gen.getAcres(df['count'], 30)
	df['year'] = year

	#### join tables to get the state abreviation #########
	df = pd.merge(df, pd.read_sql_query('SELECT atlas_st,st_abbrev FROM spatial.states;',con=engine), on='atlas_st')
	print df

	return df




def run(data, yxc, env_path, wc):
# def run():

	# addGDBTable2postgres(data,yxc)
	# createMergedTable()

	


	in_zone_data = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states'
	zone_field = 'Value'

	# Set environment settings
	env.workspace = env_path

	df_list=[]

	rasters = arcpy.ListRasters("*{}*".format(wc), "GRID")
	for in_value_raster in rasters:
		print(in_value_raster)

		###get the year from the name of the file (SUB-OPTIMAL)
		year = in_value_raster.split("_")[5]
		print year

		hist_table='{}_hist'.format(in_value_raster)
		print(hist_table)

		#ZonalHistogram(in_zone_data, zone_field, in_value_raster, hist_table)

		df_current=changeTableFormat(data, yxc, year, hist_table)

		df_list.append(df_current)

	###merge all the seperate dataframes into one.
	df_final=pd.concat(df_list)

	print 'df_final:', df_final

	df_final.to_sql('s20_ytc30_2008to2017_mmu5_fc_states', engine, schema='counts_yxc', if_exists='replace')





def pivotTableFromPostGres():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

	df = pd.read_sql_query('select * from counts_imw.merged_imw_grouped',con=engine)
    
    ###add a string to mtr value to make column names nicer
	df['mtr'] = 'mtr' + df['mtr'].astype(str)

	##perform a psuedo pivot table
	y=df.pivot(index='year', columns='mtr', values='acres')
	# df=pd.melt(df, id_vars=["year"],var_name="mtr", value_name="acres")

	print y
	# df.columns = map(str.lower, df.columns)
	y.to_sql('merged_imw_pvt', engine, schema='counts_imw', if_exists='replace')

	# return df






if __name__ == '__main__':
  # data = gen.getJSONfile()
  # run(data, 'ytc', 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s20\\post\\ytc_s20.gdb', '_fc_')
  # run()

  pivotTableFromPostGres()





