# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import json
import fnmatch


arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



def getGDBpath(wc):
    for root, dirnames, filenames in os.walk("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\"):
        for dirnames in fnmatch.filter(dirnames, '*{}*.gdb'.format(wc)):
            print dirnames
            gdbmatches = os.path.join(root, dirnames)
    print gdbmatches
    # return json.dumps(gdbmatches)
    return gdbmatches




data = gen.getJSONfile()
print data




    
def reclassifyRaster():
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 
    gdb_args_in = ['ancillary', 'cdl']
    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)

    raster = 'cdl30_2009'    
    print 'raster: ',raster

    outraster = raster.replace("_", "_b_")
    print 'outraster: ', outraster

    #define the output
    output = defineGDBpath(['pre', 'binaries'])+outraster
    print 'output: ', output

    return_string=getReclassifyValuesString(gdb_args_in[1], 'b')

    # Execute Reclassify
    arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")

    #create pyraminds
    gen.buildPyramids(output)



def getReclassifyValuesString(ds, reclass_degree):
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
    
    query = 'SELECT value::text,'+reclass_degree+' FROM misc.lookup_'+ds+' WHERE '+reclass_degree+' IS NOT NULL ORDER BY value'
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
    
    #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
    columnList = ';'.join(sum(reclassifylist, []))
    print columnList
    
    #return list to reclassifyRaster() fct
    return columnList



def getCDLlist():
    cdl_list = []
    for year in data['globals']['years']:
        print 'year:', year
        cdl_dataset = 'cdl{0}_b_{1}'.format(str(data['globals']['res']),str(year))
        cdl_list.append(cdl_dataset)
    print'cdl_list: ', cdl_list
    return cdl_list





def createTrajectories():
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order with the recalssifed nlcd raster being in the inital spot.

    # Set environment settings
    arcpy.env.workspace = getGDBpath('binaries')

    output = '\\'.join([ data['pre']['traj']['gdb'],data['pre']['traj']['filename'] ])
    print 'output', output
    
    # ###Execute Combine
    outCombine = Combine(getCDLlist())
  
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def addGDBTable2postgres():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = '\\'.join([ data['pre']['traj']['gdb'],data['pre']['traj']['filename'] ])

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(data['pre']['traj']['filename'], engine, schema='pre')
    
    #add trajectory field to table
    addTrajArrayField(fields)




def createRefinedTrajectory():

    ##### loop through each of the cdl rasters and make sure nlcd is last 
    filelist = [data['pre']['traj']['path'], data['refine']['mask_dev_alfalfa_fallow']['path'], data['refine']['mask_nlcd']['path']]

    ##### mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, data['pre']['traj_rfnd']['gdb'], data['pre']['traj_rfnd']['filename'], Raster(data['pre']['traj']['path']).spatialReference, '16_BIT_UNSIGNED', data['global']['res'], "1", "LAST","FIRST")

    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(data['pre']['traj_rfnd']['path'], "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids(data['pre']['traj_rfnd']['path'])






####  these functions create the trajectory table  #############
# createTrajectories()
# addGDBTable2postgres()
# createRefinedTrajectory()


#######  these functions are to update the lookup tables  ######
# labelTrajectories()
# FindRedundantTrajectories()
