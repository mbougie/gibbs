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





def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template




data = getJSONfile()
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



def addTrajArrayField(fields):
    #this is a sub function for addGDBTable2postgres()
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE pre.{} ADD COLUMN traj_array integer[];'.format(data['pre']['traj']['filename']));
    
    #DML: insert values into new array column
    cur.execute('UPDATE pre.{} SET traj_array = ARRAY[{}];'.format(data['pre']['traj']['filename'], columnList));
    
    conn.commit()
    print "Records created successfully";
    conn.close()


def labelTrajectories():
    cur = conn.cursor()
    table = 'pre.traj_cdl'+pre.res+'_b_'+pre.datarange
    lookuptable = 'pre.traj_'''+pre.datarange+'_lookup'

    for year in pre.conversionyears:
        pre_context = 'cdl'+pre.res+'_b_'+str(year - 2)
        before_year ='cdl'+pre.res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+pre.res+'_b_'+str(year)
        post_context = 'cdl'+pre.res+'_b_'+str(year + 1)
        query_ytc = 'update '+lookuptable+' set mtr = 3, ytc = '+str(year)+' where traj_array in (SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 0 AND '+before_year+'= 0 AND '+year_cdl+' = 1 AND '+post_context+' = 1 )'
        print query_ytc
        cur.execute(query_ytc)
        conn.commit()


        query_yfc = 'update '+lookuptable+' set mtr = 4, yfc = '+str(year)+' where traj_array in (SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 1 AND '+before_year+'= 1 AND '+year_cdl+' = 0 AND '+post_context+' = 0 )'
        print query_yfc
        cur.execute(query_yfc)
        conn.commit()



def FindRedundantTrajectories():
    # what is the purpose of this function??
    cur = conn.cursor()
    table = 'pre.traj_cdl{0}_b_{1}'.format(pre.res, pre.datarange)
    lookuptable = 'pre.traj_{}_lookup'.format(pre.datarange)

    query_list = []
    for year in pre.conversionyears:
        pre_context = 'cdl'+pre.res+'_b_'+str(year - 2)
        before_year ='cdl'+pre.res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+pre.res+'_b_'+str(year)
        post_context = 'cdl'+pre.res+'_b_'+str(year + 1)
        query_ytc = 'SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 0 AND '+before_year+'= 0 AND '+year_cdl+' = 1 AND '+post_context+' = 1'
        print query_ytc
        query_list.append(query_ytc)

        query_yfc = 'SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 1 AND '+before_year+'= 1 AND '+year_cdl+' = 0 AND '+post_context+' = 0'
        print query_yfc
        query_list.append(query_yfc)



    print query_list
    str1 = ' UNION ALL '.join(query_list)
    print str1


    query1 = 'update pre.traj_2008to2016_lookup set mtr = 5, ytc = NULL where traj_array in (select * from ('+str1+') ou where (select count(*) from ('+str1+') inr where inr.traj_array = ou.traj_array) > 1)'
    print query1
    cur.execute(query1);
    conn.commit()

    query2 = 'update pre.traj_2008to2016_lookup set mtr = 5, yfc = NULL where traj_array in (select * from ('+str1+') ou where (select count(*) from ('+str1+') inr where inr.traj_array = ou.traj_array) > 1)'
    print query2
    cur.execute(query2);
    conn.commit()







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
createRefinedTrajectory()


#######  these functions are to update the lookup tables  ######
# labelTrajectories()
# FindRedundantTrajectories()
