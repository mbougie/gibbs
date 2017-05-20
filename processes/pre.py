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



def reclassifyRaster(gdb_args, wc, reclasstable):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args)

    #loop through each of the cdl rasters
    for raster in arcpy.ListDatasets(wc, "Raster"): 
        
        print 'raster: ',raster
        # outraster = raster.replace("_", "_"+reclasstable+"_")
        outraster = reclasstable + raster[-5:]
        print 'outraster: ', outraster
        #get the arc_reclassify table
        inRemapTable = 'C:/Users/Bougie/Desktop/Gibbs/arcgis/arc_reclassify_table/'+reclasstable
        print 'inRemapTable: ', inRemapTable

        #define the output
        output = defineGDBpath(['pre','binaries'])+outraster
        print 'output: ', output

        #Execute Reclassify
        arcpy.gp.ReclassByTable_sa(raster,inRemapTable,"FROM","TO","OUT",output,"NODATA")



def createTrajectories(gdb_args,wc):

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args)
    
    #get a lsit of all rasters in sepcified database
    rasterList = arcpy.ListDatasets('*'+wc+'*', "Raster")
    
    #sort the rasterlist by accending years
    rasterList.sort(reverse=False)
    
    if wc == 'b':
        #for the binary trajectories, moves the last element in the list (i.e. nlcd_b_2011) to the first element position in the list.
        rasterList.insert(0, rasterList.pop())
    
    print 'rasterList: ',rasterList

    #Execute Combine
    outCombine = Combine(rasterList)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(['pre','trajectories'])+'traj_'+wc
    
    #Save the output 
    outCombine.save(output)



def addGDBTable2postgres(dataset):
    #set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    #path to the table you want to import into postgres
    input = defineGDBpath(['pre','trajectories'])+dataset

    #populate the fields from the atribute table into argument variable

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]

    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
     
    df = pd.DataFrame(data=arr)
    # df["rename"] = np.nan

    print df
    
    #use pandas method to import table into psotgres
    df.to_sql(dataset, engine, schema='pre')



def addTrajArrayField(degree_lc):
    cur = conn.cursor()
    arcpy.env.workspace = defineGDBpath(['pre','binaries'])
    
    #store the rasternames on defined gdb into array
    rasterList = arcpy.ListDatasets('*'+degree_lc+'*', "Raster")
    
    if degree_lc == 'b':
        #for the binary trajectories, moves the last element in the list (i.e. nlcd_b_2011) to the first element position in the list.
        rasterList.insert(0, rasterList.pop())
    
    #convert the rasterList into a string
    columnList = ','.join(rasterList)
    print columnList

    # add column to hold arrays
    cur.execute('ALTER TABLE pre.traj_' + degree_lc + ' ADD COLUMN traj_array integer[];');
    
    #insert values new array column
    cur.execute('UPDATE pre.traj_' + degree_lc + ' SET traj_array = ARRAY['+columnList+'];');
    
    conn.commit()
    print "Records created successfully";
    conn.close()



def createReclassifyList(degree_lc):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/'
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')
    df = pd.read_sql_query("select \"Value\",new_value from refinement.traj_"+degree_lc+" as a JOIN refinement.traj_lookup as b ON a.traj_array = b.traj_array WHERE b.name='"+degree_lc+"'",con=engine)
    
    print df
    a = df.values
    print a
    print type(a)

    l=a.tolist()
    print type(l)
    print l

    for raster in arcpy.ListDatasets('*'+degree_lc, "Raster"): 
        print 'raster', raster
        output = raster+'_msk'

        outReclass = Reclassify(raster, "Value", RemapRange(l), "NODATA")
        
        outReclass.save(output)



def mosaicRasters():
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb'

    pre_gdb = "C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb"
    traj = "C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb/traj"

    # Process: Mosaic To New Raster
    arcpy.MosaicToNewRaster_management("C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/traj;traj_q36_msk;traj_t61_msk;traj_tdev_msk", pre_gdb, "traj_refined50", "", "16_BIT_UNSIGNED", "", "1", "LAST", "LAST")







######  call functions  #############################
#-----reclassifyRaster(geodatabase path arguments, wildcard, arcgis reclass table)------------------
# reclassifyRaster(['ancillary','cdl'], "*", 'cdl_b')
# reclassifyRaster(['ancillary','misc'], "*nlcd*", 'nlcd_b')

#-----use arcgis combine() function to create permuations 
# createTrajectories(['pre','binaries'],"b")

#-----add the trajectories atribute table 
# addGDBTable2postgres('traj_b')

#-----add field to PG table
# addTrajArrayField('b')

#-----describe
# createReclassifyList('tdev')

#-----describe
#mosaicRasters()




