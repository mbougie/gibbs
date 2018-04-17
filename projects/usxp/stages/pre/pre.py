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



def mosiacRasters():
    arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\'

    cdl_raster=Raster("cdl30_2015")
  

    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "TOP")
    YMax = elevSTDResult.getOutput(0)
    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "BOTTOM")
    YMin = elevSTDResult.getOutput(0)
    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "LEFT")
    XMin = elevSTDResult.getOutput(0)
    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "RIGHT")
    XMax = elevSTDResult.getOutput(0)

    arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

    #### need to wrap these paths with Raster() fct or complains about the paths being a string
    
    rasterlist = ['resampled_cdl30_2007_p1','D:\\projects\\ksu\\v2\\attributes\\rasters\\cdl30_2007.img']

    ######mosiac tiles together into a new raster
    arcpy.MosaicToNewRaster_management(rasterlist, data['refine']['gdb'], data['refine']['mask_2007']['filename'], cdl_raster.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(data['refine']['mask_2007']['path'], "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids(data['refine']['mask_2007']['path'])


    
def reclassifyRaster():
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 
    gdb_args_in = ['ancillary', 'cdl']
    # Set environment settings
    arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\'

    raster = 'cdl30_2007_resampled'    
    print 'raster: ',raster

    outraster = raster.replace("_", "_b_")
    print 'outraster: ', outraster

    #define the output
    output = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\binaries.gdb\\'+outraster
    print 'output: ', output

    return_string=getReclassifyValuesString(gdb_args_in[1], 'b')

    #Execute Reclassify
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
    
    print reclassifylist
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



def addGDBTable2postgres(rasterpath, schema, tablename):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    arcpy.env.workspace = rasterpath

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(tablename)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(tablename,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(tablename, engine, schema=schema)
    
    #add trajectory field to table
    addTrajArrayField(schema, tablename, fields)




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


def createRefinedTrajectory():

    ##### loop through each of the cdl rasters and make sure nlcd is last 
    filelist = [data['pre']['traj']['path'], data['refine']['mask_dev_alfalfa_fallow']['path'], data['refine']['mask_2007']['path'], data['refine']['mask_nlcd']['path']]
    
    print 'filelist:', filelist
    
    ##### mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, data['pre']['traj_rfnd']['gdb'], data['pre']['traj_rfnd']['filename'], Raster(data['pre']['traj']['path']).spatialReference, '16_BIT_UNSIGNED', data['global']['res'], "1", "LAST","FIRST")

    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(data['pre']['traj_rfnd']['path'], "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids(data['pre']['traj_rfnd']['path'])




# reclassifyRaster()
# mosiacRasters()



####  these functions create the trajectory table  #############
# createTrajectories()
# addGDBTable2postgres('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\refine\\traj_traj.gdb\\', 'refinement_new', 'traj_try')
# createRefinedTrajectory()


#######  these functions are to update the lookup tables  ######
# labelTrajectories()
# FindRedundantTrajectories()
