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
class RefineObject:

    def __init__(self, name, res, years, nlcd_years):
        self.name = name
        self.res = res
        self.years = years
        self.yearcount=len(range(self.years[0], self.years[1]+1))

        if self.years[1] == 2016:
            self.datarange = str(self.years[0])+'to'+str(self.years[1]-1)
            print 'self.datarange:', self.datarange
            self.conversionyears = range(self.years[0]+1, self.years[1])
            print 'self.conversionyears:', self.conversionyears
        else:
            self.datarange = str(self.years[0])+'to'+str(self.years[1])
            print 'self.datarange:', self.datarange
            self.conversionyears = range(self.years[0]+1, self.years[1] + 1)
            print 'self.conversionyears:', self.conversionyears

        self.traj_dataset = "traj_cdl"+self.res+"_b_"+self.datarange
        self.nlcd_years = nlcd_years
        
     
        # self.mmu_Raster=Raster(defineGDBpath([gdb,'mtr']) + 'traj_cdl'+res+'_b_8to12_mtr')
        # def getConversionyears():
        #     if self.datarange == '2008to2012':
        #         self.conversionyears = range(self.years[0]+1, self.years[1] + 1)
        #         print self.conversionyears
        #     elif self.datarange == '2008to2016':
        #         self.conversionyears = range(self.years[0]+1, self.years[1])
        #         print self.conversionyears

        def getYXCAttributes():
            if self.name == 'ytc':
                self.mtr = '3'
                # self.subtypelist = ['sc']
                self.subtypelist = ['bfc','fc','sc']
                print 'yo', self.subtypelist
                self.traj_change = 1

        
            elif self.name == 'yfc':
                self.mtr = '4'
                self.subtypelist = ['bfnc','fnc']
                print 'yo', self.subtypelist
                # self.traj_change = 2



        
        #call functions 
        # getConversionyears()
        getYXCAttributes()


        def getCDLlist(self):
            cdl_list = []
            for year in self.data_years:
                cdl_dataset = 'cdl'+self.res+'_b_'+str(year)
                cdl_list.append(cdl_dataset)
            print'cdl_list: ', cdl_list
            return cdl_list







##############  Declare functions  ######################################################
# createMTR("traj_cdl30_b_2008to2012", ['refinement_2008to2012','mtr'])   
def createMTR():
        # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    print "-----------------createMTR() function-------------------------------"

    def createReclassifyList():
        #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

        engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
        query = 'SELECT * from pre.' + refine.traj_dataset + ' as a JOIN pre.traj_' + refine.datarange + '_lookup as b ON a.traj_array = b.traj_array'
        print 'query:', query
        df = pd.read_sql_query(query, con=engine)
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


            ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    defineGDBpath(['pre','trajectories'])

    raster = defineGDBpath(['pre','trajectories'])+refine.traj_dataset
    print 'raster:', raster

    output = defineGDBpath(['refine','mtr'])+refine.traj_dataset+'_mtr'
    print 'output:', output

    reclassArray = createReclassifyList() 

    outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

    gen.buildPyramids(output)



def createYearbinaries():
    print "-----------------createYearbinaries() function-------------------------------"
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    arcpy.env.workspace=defineGDBpath(['refine',refine.name])
    
    output = refine.name+refine.res+'_'+refine.datarange
    print 'output: ', output

    ###copy trajectory raster so it can be modified iteritively
    arcpy.CopyRaster_management(defineGDBpath(['pre', 'trajectories']) + 'traj_cdl'+refine.res+'_b_'+refine.datarange, output)
    
    arcpy.CheckOutExtension("Spatial")

    #Connect to postgres database to get values from traj dataset 
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = 'select * from pre.traj_cdl'+refine.res+'_b_'+refine.datarange+' as a JOIN pre.traj_' + refine.datarange + '_lookup as b ON a.traj_array = b.traj_array WHERE b.'+refine.name+' IS NOT NULL'
    print query
    df = pd.read_sql_query(query, con=engine)
    print 'df--',df
    
    # loop through rows in the dataframe
    for index, row in df.iterrows():
        #get the arbitrary value assigned to the specific trajectory
        value=str(row['Value'])
        print 'value: ', value

        #cy is acronym for conversion year
        cy = str(row[refine.name])
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

    removeArbitraryValuesFromYearbinaries()




def removeArbitraryValuesFromYearbinaries():
    print "-----------------removeArbitraryValuesFromYearbinaries() function-------------------------------"
    #DESCRIPTION: remove the arbitrary values from the 'yfc_years_'+mmu dataset

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(['refine',refine.name])

    arcpy.CheckOutExtension("Spatial")
    
    #get raster from geodatabse
    raster_input = refine.name+refine.res+'_'+refine.datarange
    output = refine.name+refine.res+'_'+refine.datarange+'_clean'

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


    def getAssociatedCDL(subtype, year):
        #this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function

        if subtype == 'bfc' or  subtype == 'bfnc':
            # NOTE: subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ refine.res + '_' + str(year - 1)
            return cdl_file

        elif subtype == 'fc' or  subtype == 'fnc':
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ refine.res + '_' + str(year)
            return cdl_file

        elif subtype == 'sc':
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ refine.res + '_' + str(year + 1)
            return cdl_file



    # NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
    arcpy.env.workspace=defineGDBpath(['refine',refine.name])

    inputraster = refine.name+refine.res+'_'+refine.datarange+'_clean'
    print "inputraster: ", inputraster
    
    output = inputraster+'_'+subtype
    print "output: ", output
    
    ##copy binary years raster so it can be modified iteritively
    arcpy.CopyRaster_management(inputraster, output)

    #note:checkout out spatial extension after creating a copy or issues
    arcpy.CheckOutExtension("Spatial")
 
    wc = '*'+refine.res+'*'+subtype
    print wc
  
    for year in  refine.conversionyears:
        print 'year: ', year

        # allow output to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + str(year)
        print 'cond: ', cond

        cdl_file= getAssociatedCDL(subtype, year)
        print 'associated cdl file: ', cdl_file
        
        #set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(output, cdl_file, output, cond)
   
        OutRas.save(output)

    #build pyramids t the end
    gen.buildPyramids(output)



def createChangeTrajectories():
    print "-----------------createChangeTrajectories() function-------------------------------"
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order with the recalssifed nlcd raster being in the inital spot.

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(['refine',refine.name])
    
    def getRasterList():
        orderedlist = []
        for subtype in refine.subtypelist:
            orderedlist.append(refine.name+refine.res+'_'+refine.datarange+'_clean_'+subtype)
        print orderedlist
        return orderedlist



    
    output = defineGDBpath(['refine','trajectories'])+'traj_'+refine.name+refine.res+'_'+refine.datarange
    print 'output', output

    # ##Execute Combine
    outCombine = Combine(getRasterList())
  
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def createMask_nlcdtraj():

    nlcd1 = Raster(defineGDBpath(['ancillary','nlcd'])+'nlcd'+refine.res+'_'+refine.nlcd_years[0])
    nlcd2 = Raster(defineGDBpath(['ancillary','nlcd'])+'nlcd'+refine.res+'_'+refine.nlcd_years[1])
    refinement_mtr = Raster(defineGDBpath(['refine','mtr'])+'traj_cdl'+refine.res+'_b_'+refine.datarange+'_mtr')
   

    # If condition is true set pixel to 23 else set pixel value to NULL
    #NOTE: both of the hardcoded values are ok because uniform accross resolutions and they are constant
    # outCon = Con((nlcd1 == 82) & (refinement_mtr == 3), getArbitraryCropValue(), Con((nlcd2 == 82) & (refinement_mtr == 3), getArbitraryCropValue()))
    if join_operator == 'or':
        output = defineGDBpath(['refine','masks'])+'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'or'+refine.nlcd_years[1]+'_'+refine.name+refine.datarange+'_mask'
        print 'output:', output
        outCon = Con((nlcd1 == 82) & (refinement_mtr == 3), getArbitraryCropValue(), Con((nlcd2 == 82) & (refinement_mtr == 3), getArbitraryCropValue()))
    elif join_operator == 'and':
        output = defineGDBpath(['refine','masks'])+'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'and'+refine.nlcd_years[1]+'_'+refine.name+refine.datarange+'_mask'
        print 'output:', output
        outCon = Con(((nlcd1 == 82) & (nlcd2 == 82) & (refinement_mtr == 3)), getArbitraryCropValue())
    
    outCon.save(output)

    gen.buildPyramids(output)


def getArbitraryCropValue():
    #def this is a sub function for createMask_nlcdtraj()
    #this is to return the arbitrary value assocaited with full crop over entire years span

    yotest = [1] * refine.yearcount
    print yotest
    columnList = ','.join(str(e) for e in yotest)
    print 'columnlist', columnList

    cur = conn.cursor()
   
    cur.execute("SELECT \"Value\" FROM pre.traj_cdl"+refine.res+"_b_"+refine.datarange+" Where traj_array = '{"+columnList+"}' ")

    # fetch all rows from table
    rows = cur.fetchall()
    print 'arbitrary value in ' + 'pre.traj_cdl'+refine.res+'_b_'+refine.datarange+':', rows[0][0]
    return rows[0][0]



# createMask_trajytc(['refine','trajectories'], 'traj_ytc56_2008to2012', getReclassifyValuesString())
def createMask_trajytc():
    # traj_yxc = Raster(defineGDBpath(['pre','trajectories'])+'traj_nlcd'+refine.res+'_b_2001and2006')
    traj_ytc = defineGDBpath(['refine','trajectories'])+'traj_'+refine.name+refine.res+'_'+refine.datarange
    # output = defineGDBpath(['refine','masks'])+'traj_nlcd'+refine.res+'_b_2001and2006_mask'
    # print 'output:', output

    output = defineGDBpath(['refine','masks'])+'traj_'+refine.name+refine.res+'_'+refine.datarange+'_mask'
    # output = defineGDBpath(gdb_args_out)+raster_out
    print 'output:', output

    reclassArray = createReclassifyList() 

    outReclass = Reclassify(traj_ytc, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

    gen.buildPyramids(output)



def createReclassifyList():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
   
    query = (
    "SELECT DISTINCT \"Value\" "
    "FROM refinement.traj_"+refine.name+refine.res+"_"+refine.datarange+" "
    "WHERE 122 = traj_array[1] "
    "OR 123 = traj_array[1] "
    "OR 124 = traj_array[1] "
    "OR 61 = traj_array[2] "
    "OR '{37,36,36}' = traj_array "
    "OR '{152,36,36}' = traj_array "
    "OR '{176,36,36}' = traj_array"
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
        templist.append(refine.traj_change)
        fulllist.append(templist)
    print fulllist
    return fulllist


 

def createRefinedTrajectory():
    
    traj = defineGDBpath(['pre','trajectories']) + 'traj_cdl'+refine.res+'_b_'+refine.datarange
    nlcd_mask = defineGDBpath(['refine','masks']) + 'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'or'+refine.nlcd_years[1]+'_'+refine.name+refine.datarange+'_mask'
    trajYTC_mask = defineGDBpath(['refine','masks']) + 'traj_'+refine.name+refine.res+'_'+refine.datarange+'_mask'
    output = 'traj_cdl'+refine.res+'_b_'+refine.datarange+'_rfnd'
    print 'output:', output
    
    # create a filelist to format the argument for MosaicToNewRaster_management() function
    filelist = [traj, nlcd_mask, trajYTC_mask]
    print '-----[traj, nlcd_mask, trajYTC_mask]-----'
    print filelist

    #mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, defineGDBpath(['pre','trajectories']), output, Raster(traj).spatialReference, '16_BIT_UNSIGNED', refine.res, "1", "LAST","FIRST")


def addGDBTable2postgres():
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
        # conn.close()


    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    tablename = "traj_"+refine.name+refine.res+"_"+refine.datarange
    # path to the table you want to import into postgres
    input = defineGDBpath(['refine', 'trajectories'])+tablename

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(tablename, engine, schema='refinement')
    
    #add trajectory field to table
    addTrajArrayField(tablename, fields, 'refinement')




################ Instantiate the class to create yxc object  ########################
refine = RefineObject(
      'ytc',
      '56',
      ## cdl data range---i.e. all the cdl years you are referencing 
      [2008,2016],
      ## ncdl datasets
      ['2001','2006']
      )





##################  call functions  ############################################
   
###  create the mtr directly from the trajectories without filtering  ###################
# createMTR()


### create the year conversions #####################
# createYearbinaries()


### attach the cld values to the years binaries  #######################
for subtype in refine.subtypelist:
    print subtype
    # attachCDL(subtype)


### create change trajectories for subcategories in yxc ---- OK
# createChangeTrajectories()


###  add traj_yxc[res]_[datarange] attribute to postgres database in the refinement schema ----- ok
# addGDBTable2postgres() 


### create mask  #####################
##NOTE NEED TO GENERALIZE THIS FUNCTION!!!!!!!!!!!!!!!

###----hard coded still. Also has questionable inner method!
# createMask_nlcdtraj()

###----?????  ---ok but has questionable inner method!
createMask_trajytc()


### create the refined trajectory ########################
###----hard coded still
createRefinedTrajectory()




