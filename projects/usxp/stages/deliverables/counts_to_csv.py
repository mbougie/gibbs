import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
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
# import general_deliverables as gen_dev


#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



# try:
#     conn = psycopg2.connect("dbname= 'usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
# except:
#     print "I am unable to connect to the database"

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')



def initial():
    arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\s35.gdb'
    rasters = arcpy.ListRasters("*", "GRID")
    for raster in rasters:
        print(raster)

        addRasterAttrib2postgres_specific(raster, filename='s35_combine_state_ytc_fc', database='usxp', schema='combine')





def addRasterAttrib2postgres_specific(path, filename, database, schema):
    print path
    print filename
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(database))
    
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(path)]
    print 'fields:', fields

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(path,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)


    ## add acres column
    df['acres'] = df['count'] * gen.getPixelConversion2Acres(30)
        ## add acres column
    df['hectares'] = df['count'] * gen.getPixelConversion2Hectares(30)

    print 'df-----------------------', df

    # # # use pandas method to import table into psotgres
    df.to_csv('D:\\projects\\usxp\\deliverables\\s35\\csv\\{}.csv'.format(path), index=False, header=True)
    

initial()
