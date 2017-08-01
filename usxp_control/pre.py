# Import system modules
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
import general as gen


arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


#########  global variables   ################
#acccounts for different machines having different cases in path
case=['Bougie','Gibbs']




def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 



def reclassifyRaster(gdb_args_in, wc, reclass_degree, gdb_args_out):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)

    #loop through each of the cdl rasters
    for raster in arcpy.ListDatasets('*'+wc+'*', "Raster"): 
        
        print 'raster: ',raster

        # outraster = raster.replace("_", "_"+reclasstable+"_")
        outraster = wc + '_' + reclass_degree + raster[-5:]
        print 'outraster: ', outraster
       
        #get the arc_reclassify table
        # inRemapTable = 'C:/Users/Bougie/Desktop/Gibbs/arcgis/arc_reclassify_table/'+reclasstable
        # print 'inRemapTable: ', inRemapTable

        #define the output
        output = defineGDBpath(gdb_args_out)+outraster
        print 'output: ', output

        return_string=getReclassifyValuesString(wc, reclass_degree)

        # Execute Reclassify
        arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")



def getReclassifyValuesString(wc, reclass_degree):
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()

    #DDL: add column to hold arrays
    cur.execute('SELECT value::text,'+reclass_degree+' FROM misc.lookup_'+wc+' WHERE '+reclass_degree+' IS NOT NULL ORDER BY value');
    
    #create empty list
    reclassifylist=[]

    # fetch all rows from table
    rows = cur.fetchall()
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        ww = [row[0] + ' ' + row[1]]
        reclassifylist.append(ww)
    
    #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
    columnList = ';'.join(sum(reclassifylist, []))
    print columnList
    
    #return list to reclassifyRaster() fct
    return columnList



def createTrajectories(gdb_args_in,wc,gdb_args_out):
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order with the recalssifed nlcd raster being in the inital spot.

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)
    
    #get a lsit of all rasters in sepcified database
    rasterList = arcpy.ListDatasets('*'+wc+'*', "Raster")
    
    #sort the rasterlist by accending years
    rasterList.sort(reverse=False)
    

    ##Check to see if NLCD is in the rasterlist and use pop() if it is
    if 'nlcd_b_2011' in rasterList:
        #for the binary trajectories, moves the last element in the list (i.e. nlcd_b_2011) to the first element position in the list.
        rasterList.insert(0, rasterList.pop())
    
    print 'rasterList: ',rasterList

    # Execute Combine
    outCombine = Combine(rasterList)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(gdb_args_out)+'traj_boug'+wc
    
    # #Save the output 
    outCombine.save(output)

    #create pyraminds
    # gen.buildPyramids(output)



def addGDBTable2postgres(gdb_args,wc,pg_shema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = defineGDBpath(gdb_args)+tablename

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
    
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(tablename, engine, schema=pg_shema)
    
    #add trajectory field to table
    addTrajArrayField(wc)



def addTrajArrayField(wc):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    arcpy.env.workspace = defineGDBpath(['pre','reclass'])
    
    #store the rasternames on defined gdb into array
    rasterList = arcpy.ListDatasets('*'+wc+'*', "Raster")

    rasterList.sort(reverse=False)
    
    ##Check to see if NLCD is in the rasterlist and use pop() if it is
    if 'nlcd_b_2011' in rasterList:
        #for the binary trajectories, moves the last element in the list (i.e. nlcd_b_2011) to the first element position in the list.
        rasterList.insert(0, rasterList.pop())
    
    #convert the rasterList into a string
    columnList = ','.join(rasterList)
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE pre.traj_' + wc + ' ADD COLUMN traj_array integer[];');
    
    #DML: insert values into new array column
    cur.execute('UPDATE pre.traj_' + wc + ' SET traj_array = ARRAY['+columnList+'];');
    
    conn.commit()
    print "Records created successfully";
    conn.close()








######  call functions  #############################
##-----reclassifyRaster()------------------
reclassifyRaster(['ancillary','cdl'], "cdl", "b", ['pre','binaries'])
reclassifyRaster(['ancillary','misc'], "nlcd", "b", ['pre','binaries'])

##-----createTrajectories()-----------------------------------------------
createTrajectories(['pre','binaries'], "cdl_b", ['pre','traj2'])


##-----addGDBTable2postgres()
addGDBTable2postgres(['pre','trajectories'],'cdl_b','pre')












