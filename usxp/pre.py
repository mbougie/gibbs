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


###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path





class ProcessingObject(object):

    def __init__(self, res, mmu, years):
        self.res = res
        self.mmu = mmu
        self.years = years
        self.data_years = range(self.years[0], self.years[1] + 1)
        print self.data_years
        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        print self.datarange
        print years[0]
        self.traj_dataset = "traj_cdl"+str(self.res)+"_b_"+str(self.datarange)
        

        # self.yearcount=len(range(self.years[0], self.years[1]+1))

    def __str__(self):
        return 'ProcessingObject(res: {}, years: {})'.format(self.res, str(self.years))
    

    def getCDLlist(self):
        cdl_list = []
        for year in self.data_years:
            print 'year:', year
            cdl_dataset = 'cdl'+str(self.res)+'_b_'+str(year)
            cdl_list.append(cdl_dataset)
        print'cdl_list: ', cdl_list
        return cdl_list




     

def reclassifyRaster(gdb_args_in, res, wc, reclass_degree, gdb_args_out):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)
    
    cond = gdb_args_in[1] + res + wc
    print cond
    #loop through each of the cdl rasters
    for raster in arcpy.ListDatasets(cond, "Raster"): 
        
        print 'raster: ',raster

        # outraster = raster.replace("_", "_"+reclasstable+"_")
        outraster = gdb_args_in[1] + res + '_' + reclass_degree + raster[-5:]
        print 'outraster: ', outraster
       
        #get the arc_reclassify table
        # inRemapTable = 'C:/Users/Bougie/Desktop/Gibbs/arcgis/arc_reclassify_table/'+reclasstable
        # print 'inRemapTable: ', inRemapTable

        #define the output
        output = defineGDBpath(gdb_args_out)+outraster
        print 'output: ', output

        return_string=getReclassifyValuesString(gdb_args_in[1], reclass_degree)

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


def createTrajectories():
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order with the recalssifed nlcd raster being in the inital spot.

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(['pre','binaries'])
    
    output = defineGDBpath(['pre','trajectories'])+pre.traj_dataset
    print 'output', output
    
    ###Execute Combine
    outCombine = Combine(pre.getCDLlist())
  
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def addGDBTable2postgres():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = defineGDBpath(['pre','trajectories'])+pre.traj_dataset

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(pre.traj_dataset, engine, schema='pre')
    
    #add trajectory field to table
    addTrajArrayField(fields)



def addTrajArrayField(fields):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE pre.' + pre.traj_dataset + ' ADD COLUMN traj_array integer[];');
    
    #DML: insert values into new array column
    cur.execute('UPDATE pre.' + pre.traj_dataset + ' SET traj_array = ARRAY['+columnList+'];');
    
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
    table = 'pre.traj_cdl'+pre.res+'_b_'+pre.datarange
    lookuptable = 'pre.traj_'''+pre.datarange+'_lookup'

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



###############  KEEP THIS PART OF CODE FOR NOW!!!!  ##################################

# pre = ProcessingObject(
#     #resolution
#     30,
#     #mmu
#     5,
#     #data-range
#     [2008,2016]
# )



# createTrajectories()
# addGDBTable2postgres()
# FindRedundantTrajectories()


# def run():
#     # print "pre is: {}".format(str(pre))
#     # print pre.traj_dataset
#     ###-----createTrajectories()-----------------------------------------------
#     createTrajectories()


    ###-----addGDBTable2postgres()
    # addGDBTable2postgres()


    # labelTrajectories()
    # FindRedundantTrajectories()