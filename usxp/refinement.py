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
        self.directory = 'refinement_' + self.datarange
     
        # self.mmu_Raster=Raster(defineGDBpath([gdb,'mtr']) + 'traj_cdl'+res+'_b_8to12_mtr')
        def getConversionyears():
            if self.datarange == '2008to2012':
                self.conversionyears = range(years[0]+1, years[1] + 1)
                print self.conversionyears
            elif self.datarange == '2008to2016':
                self.conversionyears = range(years[0]+1, years[1])
                print self.conversionyears

        def getYXCAttributes():
            if self.name == 'ytc':
                self.mtr = '3'
                self.subtypelist = ['fc','sc','bfc']
                print 'yo', self.subtypelist
                self.traj_change = 1

        
            elif self.name == 'yfc':
                self.mtr = '4'
                self.subtypelist = ['fnc','bfnc']
                print 'yo', self.subtypelist
                # self.traj_change = 2



        
        #call functions 
        getConversionyears()
        getYXCAttributes()



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
    arcpy.env.workspace=defineGDBpath([yxc.directory,yxc.name])
    
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
    arcpy.env.workspace=defineGDBpath([yxc.directory,yxc.name])

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
    print "-----------------attachCDL() function-------------------------------"

    # NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
    arcpy.env.workspace=defineGDBpath([yxc.directory,yxc.name])

    inputraster = yxc.name+yxc.res+'_'+yxc.datarange+'_clean'
    print "inputraster: ", inputraster
    
    output = inputraster+'_'+subtype
    print "output: ", output
    
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




def createChangeTrajectories(gdb_args_in,wc,gdb_args_out,outname):
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
    # rasterlist = ['cdl30_b_2008', 'cdl30_b_2009', 'cdl30_b_2010', 'cdl30_b_2011', 'cdl30_b_2012']
    rasterlist = ['cdl30_b_2008', 'cdl30_b_2010', 'cdl30_b_2011', 'cdl30_b_2012']
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



def createMask_nlcdtraj():

    traj_nlcd = Raster(defineGDBpath(['pre','trajectories'])+'traj_nlcd'+yxc.res+'_b_2001and2006')
    refinement_mtr = Raster(defineGDBpath([yxc.directory,'mtr'])+'traj_cdl'+yxc.res+'_b_'+yxc.datarange+'_mtr')
    output = defineGDBpath([yxc.directory,'masks'])+'traj_nlcd'+yxc.res+'_b_2001and2006_mask'
    print 'output:', output

    # If condition is true set pixel to 23 else set pixel value to NULL
    #NOTE: both of the hardcoded values are ok because uniform accross resolutions and they are constant
    outCon = Con(((traj_nlcd == 2) & (refinement_mtr == 3)), getArbitraryCropValue())

    outCon.save(output)

    gen.buildPyramids(output)


    def getArbitraryCropValue():
        #def this is a sub function for createMask_nlcdtraj()
        #this is to return the arbitrary value assocaited with full crop over entire years span

        cur = conn.cursor()
       
        cur.execute("SELECT \"Value\" FROM pre.traj_cdl"+yxc.res+"_b_"+yxc.datarange+" Where traj_array = '{1,1,1,1,1}' ")

        # fetch all rows from table
        rows = cur.fetchall()
        print 'arbitrary value in ' + 'pre.traj_cdl'+yxc.res+'_b_'+yxc.datarange+':', rows[0][0]
        return rows[0][0]



# createMask_trajytc([yxc.directory,'trajectories'], 'traj_ytc56_2008to2012', getReclassifyValuesString())
def createMask_trajytc():
    # traj_yxc = Raster(defineGDBpath(['pre','trajectories'])+'traj_nlcd'+yxc.res+'_b_2001and2006')
    traj_ytc = defineGDBpath([yxc.directory,'trajectories'])+'traj_'+yxc.name+yxc.res+'_'+yxc.datarange
    # output = defineGDBpath([yxc.directory,'masks'])+'traj_nlcd'+yxc.res+'_b_2001and2006_mask'
    # print 'output:', output

    output = defineGDBpath([yxc.directory,'masks'])+'traj_'+yxc.name+yxc.res+'_'+yxc.datarange+'_mask'
    # output = defineGDBpath(gdb_args_out)+raster_out
    print 'output:', output

    reclassArray = createReclassifyList() 

    outReclass = Reclassify(traj_ytc, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

    gen.buildPyramids(output)
    # getReclassifyValuesString()



def createReclassifyList():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
   
    query = (
    "SELECT DISTINCT Value "
    "FROM refinement.traj_"+yxc.name+yxc.res+"_"+yxc.datarange+"_table "
    "WHERE 61 = ANY(traj_array) "
    "OR 122 = ANY(traj_array) "
    "OR 123 = ANY(traj_array) "
    "OR 124 = ANY(traj_array) "
    "OR '{37,36}' = traj_array "
    "OR '{152,36}' = traj_array "
    "OR '{176,36}' = traj_array"
    )

    print query

    cur.execute(query)
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


 

def createNewMosaic():
    
    traj = defineGDBpath(['pre','trajectories']) + 'traj_cdl'+yxc.res+'_b_'+yxc.datarange
    nlcd_mask = defineGDBpath(['refinement_2008to2012','masks']) + 'traj_nlcd'+yxc.res+'_b_2001and2006_mask'
    trajYTC_mask = defineGDBpath(['refinement_2008to2012','masks']) + 'traj_'+yxc.name+yxc.res+'_'+yxc.datarange+'_mask'
    output = 'traj_cdl'+yxc.res+'_b_'+yxc.datarange+'_rfnd'
    
    # create a filelist to format the argument for MosaicToNewRaster_management() function
    filelist = [traj, nlcd_mask, trajYTC_mask]
    print filelist

    #mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, defineGDBpath(['pre','trajectories']), output, Raster(traj).spatialReference, "8_BIT_UNSIGNED", yxc.res, "1", "LAST","FIRST")


def addGDBTable2postgres():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    tablename = 'traj_ytc30_2008to2012_table'
    # path to the table you want to import into postgres
    input = defineGDBpath([yxc.directory, 'trajectories'])+tablename

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    # df.to_sql(tablename, engine, schema='refinement')
    
    #add trajectory field to table
    addTrajArrayField(tablename, fields, 'refinement')



    def addTrajArrayField(tablename, fields, schema):
        #this is a sub function for addGDBTable2postgres()
        
        cur = conn.cursor()
        
        #convert the rasterList into a string
        columnList = ','.join(fields[3:])
        print columnList

        #DDL: add column to hold arrays
        cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN traj_array integer[];');
        
        #DML: insert values into new array column
        cur.execute('UPDATE ' + schema + '.' + tablename + ' SET traj_array = ARRAY['+columnList+'];');
        
        conn.commit()
        print "Records created successfully";
        conn.close()


################ Instantiate the class to create yxc object  ########################
yxc = ConversionObject(
      'ytc',
      '30',
      ## data range---i.e. all the cdl years you are referencing 
      [2008,2012]
      )





##################  call functions  ############################################
   
###  create the mtr directly from the trajectories without filtering  ###################
# createMTR(['pre','trajectories'],"traj_cdl30_b_2008to2012", ['refinement_2008to2012','mtr'])


### create the year conversions #####################
# createYearbinaries()
# removeArbitraryValuesFromYearbinaries()


### attach the cld values to the years binaries  #######################
for subtype in yxc.subtypelist:
    print subtype
    # attachCDL(subtype)


### create change trajectories for subcategories in yxc 
#createChangeTrajectories()


###  sebd traj_yxc[res]_[datarange] attribute to postgres database in the refinement schema   ########################
# addGDBTable2postgres()


### create mask  #####################
# createMask_nlcdtraj()
# createMask_trajytc()


### create the refined trajectory ########################
createNewMosaic()




