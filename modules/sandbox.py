from sqlalchemy import create_engine
import numpy as np, sys, os
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
import general as gen

'''######## DEFINE THESE EACH TIME ##########'''


#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

###################  declare functions  #######################################################
#establish root path for this the main project (i.e. usxp)
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


# gen.importCSVtoPG()

# gen.getPGColumnsList("'refinement'", "'counties_yfc_bfnc'", " , ")


# gen.transposeTable(['refinement','refinement'],'counties_yfc_years')



# def addGDBTable2postgres(gdb_args,tablename,pg_shema):
#     # set the engine.....
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
#     tablename = 'traj_'+wc
#     # path to the table you want to import into postgres
#     input = defineGDBpath(gdb_args)+tablename

#     # Execute AddField twice for two new fields
#     fields = [f.name for f in arcpy.ListFields(input)]
   
#     # converts a table to NumPy structured array.
#     arr = arcpy.da.TableToNumPyArray(input,fields)
#     print arr
    
#     # convert numpy array to pandas dataframe
#     df = pd.DataFrame(data=arr)

#     print df
    
#     # use pandas method to import table into psotgres
#     df.to_sql(tablename, engine, schema=pg_shema)
    
#     #add trajectory field to table
#     addTrajArrayField(tablename, fields)



def addGDBTable2postgres(gdb_args,wc,pg_shema):
	print 'running addGDBTable2postgres() function....'
	####description: adds tables in geodatabse to postgres
	# set the engine.....
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

	arcpy.env.workspace = defineGDBpath(gdb_args)

	for table in arcpy.ListTables(wc): 
		print 'table: ', table

		# Execute AddField twice for two new fields
		fields = [f.name for f in arcpy.ListFields(table)]

		# converts a table to NumPy structured array.
		arr = arcpy.da.TableToNumPyArray(table,fields)
		print arr

		# convert numpy array to pandas dataframe
		df = pd.DataFrame(data=arr)

		print df

		df.columns = map(str.lower, df.columns)

		# use pandas method to import table into psotgres
		df.to_sql(table, engine, schema=pg_shema)

		#add trajectory field to table
		addTrajArrayField(table, fields)





def addTrajArrayField(tablename, fields):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE refinement.' + tablename + ' ADD COLUMN traj_array integer[];');
    
    #DML: insert values into new array column
    cur.execute('UPDATE refinement.' + tablename + ' SET traj_array = ARRAY['+columnList+'];');
    
    conn.commit()
    print "Records created successfully";
    conn.close()




addGDBTable2postgres(['refinement','refinement_current'],'*ytc30*','refinement')






# create table refinement.traj_ytc30_8to12_table_lookup as  

# select distinct traj_array
# from refinement.traj_ytc30_8to12_table 
# where 61 = ANY(traj_array) 
# OR 122 = ANY(traj_array)
# OR 123 = ANY(traj_array)
# OR 124 = ANY(traj_array)
# OR '{37,36}' = traj_array
# OR '{152,36}' = traj_array
# OR '{176,36}' = traj_array



