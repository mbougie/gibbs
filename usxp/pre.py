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


#################### class to create yxc object  ####################################################
class ConversionObject:

    def __init__(self, name, subtype, conversionyears):
        self.name = name
        self.subtype = subtype
        self.conversionyears = range(conversionyears[0], conversionyears[1] + 1)
        # self.mmu_gdb=defineGDBpath(['core','mmu'])
        # self.mmu='traj_cdl_b_n8h_mtr_8w_msk23_nbl'
        # self.mmu_Raster=Raster(self.mmu_gdb + self.mmu)
        

        # if self.name == 'ytc':
        #     self.mtr = '3'
        # elif self.name == 'yfc':
        #     self.mtr = '4'
    
    #function for to get correct cdl for the attachCDL() function
    def getAssociatedCDL(self, year):
        if self.subtype == 'bfc' or  self.subtype == 'bfnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl_'+ str(year - 1)
            return cdl_file

        elif self.subtype == 'fc' or  self.subtype == 'fnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl_'+ str(year)
            return cdl_file

    def createCompareList(self):
        cdl_binaries = []
        for n in self.conversionyears:
            cdl_binaries.append('cdl_b_'+str(n))
        print cdl_binaries
        return cdl_binaries
        #     print n 

        # # set(a) & set(b)

        # if any(str(range(2008,2015)) in s for s in rasterList):
        #    print 'hi'
        # #sort the rasterlist by accending years
        # rasterList.sort(reverse=False)

        




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



def createTrajectories(gdb_args_in,wc,gdb_args_out,outname):
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order with the recalssifed nlcd raster being in the inital spot.

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)
    
    #get a lsit of all rasters in sepcified database
    rasterlist = arcpy.ListRasters('*'+wc+'*')
    print rasterlist
    rasterlist.sort(reverse=False)
    print rasterlist
    

    ####NOTE GENERIC!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    rasterlist = ['cdl30_b_2008', 'cdl30_b_2009', 'cdl30_b_2010', 'cdl30_b_2011', 'cdl30_b_2012']
    print rasterlist
    
    ###Execute Combine
    outCombine = Combine(rasterlist)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(gdb_args_out)+outname
    print 'output', output
    
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def addGDBTable2postgres(gdb_args,tablename,pg_shema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
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
    addTrajArrayField(tablename, fields)



def addTrajArrayField(tablename, fields):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE pre.' + tablename + ' ADD COLUMN traj_array integer[];');
    
    #DML: insert values into new array column
    cur.execute('UPDATE pre.' + tablename + ' SET traj_array = ARRAY['+columnList+'];');
    
    conn.commit()
    print "Records created successfully";
    conn.close()




yxc = ConversionObject(
  'ytc',
  'bfc', 
  [2008,2012]
  )



######  call functions  #############################
###-----reclassifyRaster()------------------
# reclassifyRaster(['ancillary','cdl'], "30", "*2008*", "b", ['pre','binaries'])

###-----createTrajectories()-----------------------------------------------
createTrajectories(['pre','binaries'], "cdl30", ['pre','trajectories'], 'traj_cdl30_b_2008to2012')


###-----addGDBTable2postgres()
addGDBTable2postgres(['pre','trajectories'],'traj_cdl30_b_2008to2012','pre')












