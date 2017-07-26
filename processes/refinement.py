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

'''
Description---
This script is meant to refine the intial trajectory by removing false change from each landcover defined.
'''

# set the engine for psndas
engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

# set con for psycopg2
try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path



def PG_DDLandDML(degree_lc):

    #define cursor
	cur = conn.cursor()
    
    # add column to table to hold arrays
	cur.execute('ALTER TABLE pre.traj_' + degree_lc + ' ADD COLUMN traj_array integer[];');
    
    # insert values into array column
	cur.execute('UPDATE pre.traj_' + degree_lc + ' SET traj_array = ARRAY[nlcd_b_2011,cdl_' + degree_lc + '_2010,cdl_' + degree_lc + '_2011,cdl_' + degree_lc + '_2012,cdl_' + degree_lc + '_2013,cdl_' + degree_lc + '_2014,cdl_' + degree_lc + '_2015,cdl_' + degree_lc + '_2016];');
    
    #commit the changes
	conn.commit()
	print "Records created successfully";

	#close..........
	conn.close()



def createTrajMask(degree_lc):
    arcpy.env.workspace = defineGDBpath(['pre','trajectories'])

    df = pd.read_sql_query("select a.\"Value\",b.mtr from pre.traj_"+degree_lc+" as a JOIN pre.traj_r_lookup as b ON a.traj_array = b.traj_array",con=engine)
    
    print df
    a = df.values
    print a
    print type(a)

    l=a.tolist()
    print type(l)
    print l

    for raster in arcpy.ListDatasets('*'+degree_lc, "Raster"): 
        print 'in raster: ', raster
        output = raster+'_msk'
        print 'output raster: ', output
        outReclass = Reclassify(raster, "Value", RemapRange(l), "NODATA")
        
        outReclass.save(output)



def mosaicRasters():

    ##STILL NEED TO DEVLOP
    arcpy.env.workspace = defineGDBpath(['pre','trajectories'])

    # Execute Con
    outCon = Con(IsNull('traj_r_msk'), 'traj_b', 'traj_r_msk')

    outCon.save("traj")




def reclassifyRaster():
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(['ancillary','cdl'])

    for raster in arcpy.ListDatasets('*', "Raster"): 
        print 'raster:', raster

        outraster = raster.replace("_", "_r_")

        print outraster 

        #define the output
        output = defineGDBpath(['pre','reclass'])+outraster
        print 'output: ', output

        return_string=getReclassifyValuesString()

        # Execute Reclassify
        arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")



def getReclassifyValuesString():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()

    #DDL: add column to hold arrays
    cur.execute('SELECT value::text,test FROM misc.lookup_cdl WHERE test IS NOT NULL ORDER BY value');
    
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



def createTrajectories(wc):

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(['pre','reclass'])
    
    #get a lsit of all rasters in sepcified database
    rasterList = arcpy.ListDatasets('cdl_'+wc+'*', "Raster")
    
    #sort the rasterlist by accending years
    rasterList.sort(reverse=False)
    
    #prepend nlcd raster name 
    rasterList.insert(0, 'nlcd_b_2011')
    print 'rasterList: ',rasterList

    #Execute Combine
    outCombine = Combine(rasterList)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(['pre','trajectories'])+'traj_'+wc
    
    #Save the output 
    outCombine.save(output)



def addGDBTable2postgres(gdb_args,tablename,pg_shema):


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







######  call functions  #############################
reclassifyRaster()
createTrajectories("r")
addGDBTable2postgres(['pre','trajectories'],'traj_r','pre')
PG_DDLandDML('r')
createTrajMask('r')
mosaicRasters()





