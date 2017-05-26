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

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)
    
    #get a lsit of all rasters in sepcified database
    rasterList = arcpy.ListDatasets('*'+wc+'*', "Raster")
    
    #sort the rasterlist by accending years
    rasterList.sort(reverse=False)
    
    if wc == wc:
        #for the binary trajectories, moves the last element in the list (i.e. nlcd_b_2011) to the first element position in the list.
        rasterList.insert(0, rasterList.pop())
    
    print 'rasterList: ',rasterList

    #Execute Combine
    outCombine = Combine(rasterList)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(gdb_args_out)+'traj_'+wc
    
    #Save the output 
    outCombine.save(output)



def addGDBTable2postgres(gdb_args,tablename,pg_shema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

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

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE pre.traj_' + degree_lc + ' ADD COLUMN traj_array integer[];');
    
    #DML: insert values into new array column
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
#-----reclassifyRaster(gdb_args_in, wc, reclass_degree, gdb_args_out)------------------
# reclassifyRaster(['ancillary','cdl'], "cdl", "test", ['pre','test'])
# reclassifyRaster(['ancillary','misc'], "nlcd", "b", ['pre','binaries'])

#-----createTrajectories(gdb_args_in,wc,gdb_args_out)-----------------------------------------------
# createTrajectories(['pre','test'], "test", ['pre','trajectories'])

#-----add the trajectories atribute table to postgres
# addGDBTable2postgres(['pre','trajectories'],'traj_b','pre')

#-----add field to PG table
# addTrajArrayField('b')

#-----describe
# createReclassifyList('tdev')

#-----NOTE: this a a refinement function to mosiac all the rasters!
#mosaicRasters()







# addGDBTable2postgres(['ancillary','misc'],'nlcd_2011','pre')


# addGDBTable2postgres(['pre','trajectories'],'traj_b_counts','pre')





# C:\ProgramData\Oracle\Java\javapath;%SystemRoot%\system32;%SystemRoot%;%SystemRoot%\System32\Wbem;%SYSTEMROOT%\System32\WindowsPowerShell\v1.0\;C:\Python27\ArcGISx6410.4;C:\Python27\ArcGISx6410.4\Scripts;C:\Python27\ArcGISx6410.4\Lib\site-packages;C:\Python27\ArcGISx6410.4\Lib\site-packages\osgeo;C:\Program Files\Git\cmd;C:\Program Files (x86)\Skype\Phone\



# C:\ProgramData\Oracle\Java\javapath;%SystemRoot%\system32;%SystemRoot%;%SystemRoot%\System32\Wbem;%SYSTEMROOT%\System32\WindowsPowerShell\v1.0\;C:\Python27\ArcGISx6410.4;C:\Python27\ArcGISx6410.4\Scripts;C:\Python27\ArcGISx6410.4\Lib\site-packages;C:\Python27\ArcGISx6410.4\Lib\site-packages\osgeo;C:\Program Files\Git\cmd;C:\Program Files (x86)\Skype\Phone\