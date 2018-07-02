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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen



'''######## DEFINE THESE EACH TIME ##########'''



#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



try:
    conn = psycopg2.connect("dbname= 'usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




def addGDBTable2postgres():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'
    raster = 'D:\\projects\\usxp\\deliverables\\s27\\maps\\choropleths\\usxp\\usxp.gdb\\s27_mtr4_counties'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(raster)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(raster,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    schema = 'deliverables'
    tablename = 's27_mtr4_counties'
    
    # # # use pandas method to import table into psotgres
    df.to_sql(tablename, engine, schema=schema)
    
    # # #add trajectory field to table
    addAcresField(schema, tablename, '30')



def addAcresField(schema, tablename, res):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    ####DDL: add column to hold arrays
    query = 'ALTER TABLE {}.{} ADD COLUMN acres bigint'.format(schema, tablename)
    print query
    cur.execute(query)

    print int(tablename.split("_")[0][1:])

    #####DML: insert values into new array column
    cur.execute("UPDATE {0}.{1} SET acres=count*{2}".format(schema, tablename, gen.getPixelConversion2Acres(res) ))
    conn.commit() 





addGDBTable2postgres()