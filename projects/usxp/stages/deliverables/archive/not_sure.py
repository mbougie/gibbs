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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 












def addGDBTable2postgres(gdb, table, schema, pixelcount):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    arcpy.env.workspace = gdb

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(table)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(table,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)
    
    df['percent'] = (df['value']/pixelcount)*100

    print df


    # use pandas method to import table into psotgres
    # df.to_sql(table, engine, schema=schema, if_exists='replace')
    

def reclassifyRaster(gdb, raster):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = gdb

    #define the output
    output = 'test_reclass_yo'
    print 'output: ', output

    return_string=getReclassifyValuesString()

    #Execute Reclassify
    arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")

    #create pyraminds
    gen.buildPyramids(output)



def getReclassifyValuesString():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
    
    query = 'SELECT value::text, percent::text FROM deliverables.bs_200_mtr3'
    
    #DDL: add column to hold arrays
    cur.execute(query);
    
    #create empty list
    reclassifylist=[]

    # fetch all rows from table
    rows = cur.fetchall()
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        ww = [row[0] + ' ' + row[1]]
        reclassifylist.append(ww)
    
    print reclassifylist
    #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
    columnList = ';'.join(sum(reclassifylist, []))
    print columnList
    
    #return list to reclassifyRaster() fct
    return columnList





# addGDBTable2postgres('D:\\projects\\usxp\\current_deliverable\\5_23_18\\deliverables.gdb', 'bs_200_mtr3', 'deliverables', 10000)

reclassifyRaster('D:\\projects\\usxp\\current_deliverable\\5_23_18\\deliverables.gdb', 'bs_200_mtr3')







