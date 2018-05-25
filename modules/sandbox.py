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



###################  declare functions  #######################################################

db = 'side_projects'


# db = 'ksu'
# rootpath = 'D:/projects/'


### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = 'D:\\projects\\usxp\\series\\s5\\xp_update_refined.gdb'
    print 'gdb path: ', gdb_path 
    return gdb_path






try:
    conn = psycopg2.connect("dbname= "+db+" user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"





def addGDBTable2postgres(yxc, schema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'
    raster = 'D:\\projects\\usxp\\series\\s9\\ytc.gdb\\s9_ytc30_2008to2016_mmu5_nbl_fc'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(raster)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(raster,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    tablename = 's9_ytc30_2008to2016_mmu5_nbl_fc'
    
    # # # use pandas method to import table into psotgres
    df.to_sql(tablename, engine, schema=schema)
    
    # #add trajectory field to table
    addAcresField(schema, tablename, yxc, '30')







def addAcresField(schema, tablename, yxc, res):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    ####DDL: add column to hold arrays
    query = 'ALTER TABLE {}.{} ADD COLUMN acres bigint, ADD COLUMN series text, ADD COLUMN yxc text, ADD COLUMN series_order integer'.format(schema, tablename)
    print query
    cur.execute(query)

    print int(tablename.split("_")[0][1:])

    #####DML: insert values into new array column
    cur.execute("UPDATE {0}.{1} SET acres=count*{2}, series='{3}', yxc='{4}', series_order={5}".format(schema, tablename, gen.getPixelConversion2Acres(res), tablename.split("_")[0], yxc, int(tablename.split("_")[0][1:])))
    conn.commit() 





def importCSVtoPG():

    df = pd.read_excel('C:\\Users\\Bougie\\Downloads\\noncropland_cropland_county.csv')
    df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces

    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/usxp')

    df.to_sql("fsa_2012", engine, schema='sa')




def createMergedTable():
  cur = conn.cursor()
  query="SELECT table_name FROM information_schema.tables WHERE table_schema = 'counts_yxc' AND SUBSTR(table_name, 1, 1) = 's';"
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



def addGDBTable2postgres_w_array(parent, filename, schema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/side_projects')
    
    # arcpy.env.workspace = data['pre']['traj']['path']

    tablename=parent+filename

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(tablename)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(tablename,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    # df.to_sql(filename, engine, schema=schema)
    
    #add trajectory field to table
    addTrajArrayField(schema, filename, fields)




def addTrajArrayField(schema, tablename, fields):
    #this is a sub function for addGDBTable2postgres()
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE {}.{} ADD COLUMN traj_array integer[];'.format(schema, tablename));
    
    #DML: insert values into new array column
    cur.execute('UPDATE {}.{} SET traj_array = ARRAY[{}];'.format(schema, tablename, columnList));
    
    conn.commit()
    print "Records created successfully";
    conn.close()







a = np.array([288128028,205651,41170,1165935,1086902,864040,854994,823598,448141])
print a
print np.cumsum(a)









##############  call functions  #############################################


# addGDBTable2postgres_w_array('D:\\projects\\map_biomas\\Test3_subSet\\Test3_subSet\\', 'combine_sc_22', 'mapbiomas')
# createMergedTable()
