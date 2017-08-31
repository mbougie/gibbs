from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
import general as gen 


'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



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

    def __init__(self, name, res, years):
        self.name = name
        # self.subtype = subtype
        self.res = res
        self.datarange = str(years[0])+'to'+str(years[1])
        print self.datarange
        self.gdb = 'refinement_' + self.datarange
        self.conversionyears = range(years[0]+1, years[1] + 1)
        print self.conversionyears
        # self.mmu_Raster=Raster(defineGDBpath([gdb,'mtr']) + 'traj_cdl'+res+'_b_8to12_mtr')
        

        if self.name == 'ytc':
            self.mtr = '3'
            self.subtypelist = ['fc','bfc']
            # self.traj_nlcd = 24
            # self.traj_change = 1
            

        elif self.name == 'yfc':
            self.mtr = '4'
            self.subtypelist = ['fnc','bfnc']
            # self.reclass = '24'




##############  Declare functions  ######################################################
    
def createMTR(gdb_args_in, traj_dataset, gdb_args_out):
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    arcpy.env.workspace = defineGDBpath(gdb_args_in)

    for raster in arcpy.ListDatasets(traj_dataset+'*', "Raster"): 
        print 'raster:', raster
        raster_out = raster+'_mtr'
        output = defineGDBpath(gdb_args_out)+raster_out
        print 'output:', output

        reclassArray = createReclassifyList(traj_dataset) 

        outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        gen.buildPyramids(output)



def createReclassifyList(traj_dataset):
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('SELECT "Value", mtr from pre.' + traj_dataset + ' as a JOIN pre.' + traj_dataset + '_lookup as b ON a.traj_array = b.traj_array',con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['mtr']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist



def createYearbinaries():
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    arcpy.env.workspace=defineGDBpath([yxc.gdb,yxc.name])
    
    output = yxc.name+yxc.res+'_'+yxc.datarange
    print 'output: ', output

    ###copy trajectory raster so it can be modified iteritively
    # arcpy.CopyRaster_management(defineGDBpath(['pre', 'trajectories']) + 'traj_cdl'+yxc.res+'_b_'+yxc.datarange, output)
    
    arcpy.CheckOutExtension("Spatial")

    #Connect to postgres database to get values from traj dataset 
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('select * from pre.traj_cdl'+yxc.res+'_b_'+yxc.datarange+' as a JOIN pre.traj_cdl'+yxc.res+'_b_'+yxc.datarange+'_lookup as b ON a.traj_array = b.traj_array WHERE b.'+yxc.name+' IS NOT NULL',con=engine)
    print 'df--',df
    
    # loop through rows in the dataframe
    for index, row in df.iterrows():
        #get the arbitrary value assigned to the specific trajectory
        value=str(row['Value'])
        print 'value: ', value

        #cy is acronym for conversion year
        cy = str(row[yxc.name])
        print 'cy:', cy
        
        # allow raster to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + value
        print 'cond: ', cond
        
        # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(output, cy, output, cond)
   
        OutRas.save(output)

    #build pyramids t the end
    gen.buildPyramids(output)




def removeArbitraryValuesFromYearbinaries():
    #DESCRIPTION: remove the arbitrary values from the 'yfc_years_'+mmu dataset

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath([yxc.gdb,yxc.name])

    arcpy.CheckOutExtension("Spatial")
    
    #get raster from geodatabse
    raster_input = yxc.name+yxc.res+'_'+yxc.datarange
    output = yxc.name+yxc.res+'_'+yxc.datarange+'_clean'

    ##only keep year values in map
    cond = "Value < 2009" 
    print 'cond: ', cond
        
    # set mmu raster to null where value is less 2013 (i.e. get rid of attribute values)
    outSetNull = SetNull(raster_input, raster_input,  cond)
    
    #Save the output 
    outSetNull.save(output)

    #build pyramids t the end
    gen.buildPyramids(output)



def attachCDL(subtype):
    # DESCRIPTION:attach the appropriate cdl value to each year binary dataset

    # NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
    arcpy.env.workspace=defineGDBpath([yxc.gdb,yxc.name])

    inputraster = yxc.name+yxc.res+'_'+yxc.datarange
    output = inputraster+'_'+subtype
    
    ###copy binary years raster so it can be modified iteritively
    arcpy.CopyRaster_management(inputraster, output)

    #note:checkout out spatial extension after creating a copy or issues
    arcpy.CheckOutExtension("Spatial")
 
    wc = '*'+yxc.res+'*'+subtype
    print wc
  
    for year in  yxc.conversionyears:
        print 'output: ', output
        print 'year: ', year

        # allow output to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + str(year)
        print 'cond: ', cond

        cdl_file= getAssociatedCDL(subtype, year)
        print 'associated cdl file: ', cdl_file
        
        # # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(output, cdl_file, output, cond)
   
        OutRas.save(output)

    #build pyramids t the end
    gen.buildPyramids(output)



def getAssociatedCDL(subtype, year):
#this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function

    if subtype == 'bfc' or  subtype == 'bfnc':
        # NOTE: subtract 1 from every year in list
        cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ yxc.res + '_' + str(year - 1)
        return cdl_file

    elif subtype == 'fc' or  subtype == 'fnc':
        cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ yxc.res + '_' + str(year)
        return cdl_file



def createChangeTrajectories():
    print 'new function'







def getReclassifyValuesString():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
    #DDL: add column to hold arrays
    cur.execute('select value from refinement.traj_'+yxc.name+yxc.res+'_'+yxc.datarange+'_table as a JOIN refinement.traj_'+yxc.name+yxc.res+'_'+yxc.datarange+'_table_lookup as b ON a.traj_array = b.traj_array')
    
    #create empty list
    fulllist=[[0,0,"NODATA"]]

    # fetch all rows from table
    rows = cur.fetchall()
    print 'number of records in lookup table', len(rows)
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        templist=[]
        templist.append(row[0])
        templist.append(yxc.traj_change)
        fulllist.append(templist)
    print fulllist
    return fulllist




def createMask(gdb_path, traj_dataset, reclassArray):
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    arcpy.env.workspace = defineGDBpath(gdb_path)

    # arcpy.CheckOutExtension("Spatial")

    for raster in arcpy.ListDatasets(traj_dataset, "Raster"): 
        print 'raster:', raster
        output = defineGDBpath([yxc.gdb,'masks'])+traj_dataset+'_mask'
        # output = defineGDBpath(gdb_args_out)+raster_out
        print 'output:', output

        # reclassArray = createReclassifyList(traj_dataset) 
        # reclassArray = [[0, 0, 'NODATA'], [10000, 1], [2400, 23]]

        outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        gen.buildPyramids(output)


################ Instantiate the class to create yxc object  ########################
yxc = ConversionObject(
      'ytc',
      '30',
      ## data range---i.e. all the cdl years you are referencing 
      [2008,2012]
      )





##################  call functions  ############################################
   
###  core  ###################
# createMTR(['pre','trajectories'],"traj_cdl30_b_2008to2012", ['refinement_2008to2012','mtr'])




###  post  ####################
# createYearbinaries()
# removeArbitraryValuesFromYearbinaries()


for subtype in yxc.subtypelist:
    print subtype
    attachCDL(subtype)




#createChangeTrajectories()

# createMask(['pre','trajectories'], 'traj_nlcd56_b_2001and2006', [[0, 0, 'NODATA'], [2, 23]])
# createMask([yxc.gdb,'trajectories'], 'traj_ytc56_2008to2012', getReclassifyValuesString())


