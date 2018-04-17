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
    gdb_path = '{}{}/{}/{}.gdb/'.format(rootpath,arg_list[0],arg_list[1],arg_list[2])
    print 'gdb path: ', gdb_path 
    return gdb_path




#################### class to create yxc object  ####################################################

 
class ProcessingObject(object):

    def __init__(self, series, res, years, name, join_operator, mask_dev, nlcd_years):

        self.series = series
        self.res = str(res)
        self.years = years
        self.name = name
        self.join_operator = join_operator
        self.mask_dev = mask_dev
        self.nlcd_years = nlcd_years

        ##### years objects
        self.yearcount=len(range(self.years[0], self.years[1]+1))
        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        print 'self.datarange:', self.datarange
        self.conversionyears = range(self.years[0]+2, self.years[1])
        print 'self.conversionyears:', str(self.conversionyears)
        
        ##### derived datsets
        self.traj_dataset = "traj_cdl"+self.res+"_b_"+self.datarange
        self.traj_rfnd_dataset = self.series+"_traj_cdl"+self.res+"_b_"+self.datarange+'_rfnd'
        
        self.traj_dataset_path = defineGDBpath(['pre', 'v2', 'trajectories']) + self.traj_dataset
        # self.yxc_dataset = self.name+self.res+'_'+self.datarange

        self.yxc_dataset = self.name+self.res+'_'+self.datarange
        print 'self.yxc_dataset:', self.yxc_dataset 

        #### nlcd datasets 
        self.nlcd_ven = 'or'.join(self.nlcd_years)
        print 'self.nlcd_ven:', self.nlcd_ven
        self.nlcd_count = len(self.nlcd_years)
        print 'self.nlcd_count:', self.nlcd_count

        def getYXCAttributes():
            if self.name == 'ytc':
                self.mtr = '3'
                self.subtypelist = ['bfc','fc']
                # self.subtypelist = ['bfc']
                print 'yo', self.subtypelist
                self.traj_change = 1

        
            elif self.name == 'yfc':
                self.mtr = '4'
                # self.subtypelist = ['bfnc','fnc']
                self.subtypelist = ['fnc']
                print 'yo', self.subtypelist



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
            templist.append(int(row['Value']))
            templist.append(int(row['mtr']))
            fulllist.append(templist)
        print 'fulllist: ', fulllist
        return fulllist


    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    reclassArray = createReclassifyList() 

    raster = Raster(defineGDBpath(['pre','trajectories'])+refine.traj_dataset)
    print 'raster:', raster

    output = defineGDBpath(['refine','mtr'])+refine.series+'_'+refine.traj_dataset+'_mtr'
    print 'output:', output

    outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

    gen.buildPyramids(output)




def createYearbinaries():
        # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    print "-----------------  createYearbinaries()  -------------------------------"

    def createReclassifyList():
        #this is a sub function for createYearbinaries_better().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

        engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
        query = 'SELECT * from pre.traj_cdl'+refine.res+"_b_"+refine.datarange + ' as a JOIN pre.traj_' + refine.datarange + '_lookup as b ON a.traj_array = b.traj_array WHERE '+refine.name+' IS NOT NULL'
        print 'query:', query
        df = pd.read_sql_query(query, con=engine)
        print df
        fulllist=[[0,0,"NODATA"]]
        for index, row in df.iterrows():
            templist=[]
            value=row['Value'] 
            yxc=row[refine.name]  
            templist.append(int(value))
            templist.append(int(yxc))
            fulllist.append(templist)
        print 'fulllist: ', fulllist
        return fulllist


    ## replace the arbitrary values in the trajectories dataset with the yxc values.
    reclassArray = createReclassifyList()

    raster = Raster(defineGDBpath(['pre','trajectories'])+refine.traj_dataset)
    print 'raster:', raster

    output = defineGDBpath(['refine',refine.name])+refine.yxc_dataset
    print 'output: ', output

    outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

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





    # NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
    arcpy.env.workspace=defineGDBpath(['refine',refine.name])

    inputraster = defineGDBpath(['refine',refine.name])+refine.yxc_dataset

    print "inputraster: ", inputraster
    
    output = inputraster+'_'+subtype
    print "output: ", output
    
    ##copy binary years raster so it can be modified iteritively
    arcpy.CopyRaster_management(inputraster, output)

    #note:checkout out spatial extension AFTER creating a copy or issues
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
    arcpy.env.workspace = defineGDBpath(['refine','v2',refine.name])
    
    def getRasterList():
        orderedlist = []
        for subtype in refine.subtypelist:
            orderedlist.append(refine.name+refine.res+'_'+refine.datarange+'_'+subtype)
        print orderedlist
        return orderedlist



    
    output = defineGDBpath(['refine','v2','trajectories'])+'traj_'+refine.name+refine.res+'_'+refine.datarange
    print 'output----', output

    # ##Execute Combine
    outCombine = Combine(getRasterList())
  
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def createMask_nlcd():

    nlcd2001 = Raster(defineGDBpath(['ancillary','raster','nlcd'])+'nlcd'+refine.res+'_2001')
    nlcd2006 = Raster(defineGDBpath(['ancillary','raster','nlcd'])+'nlcd'+refine.res+'_2006')
    nlcd2011 = Raster(defineGDBpath(['ancillary','raster','nlcd'])+'nlcd'+refine.res+'_2011')
    refinement_mtr = Raster(defineGDBpath(['refine','v2','mtr'])+'traj_cdl'+refine.res+'_b_'+refine.datarange+'_mtr')
    print 'refinement_mtr:', refinement_mtr
   
    if refine.join_operator == 'or':
        if refine.nlcd_count == 2:
            output = defineGDBpath(['refine','v2','masks'])+refine.traj_dataset+'_nlcd'+refine.nlcd_ven
            print 'output:', output
            outCon = Con((nlcd2006 == 82) & (refinement_mtr == 3) & (refine.yxc_dataset <= 2011), getArbitraryCropValue(), Con((nlcd2006 == 82) & (refinement_mtr == 3) & (refine.yxc_dataset > 2011), getArbitraryCropValue())) 
            print outCon
        if refine.nlcd_count == 3:
            output = defineGDBpath(['refine','v2','masks'])+refine.traj_dataset+'_nlcd'+refine.nlcd_ven
            print output
            outCon = Con((nlcd2001 == 82) & (refinement_mtr == 3) & (refine.yxc_dataset < 2012), getArbitraryCropValue(), Con((nlcd2006 == 82) & (refinement_mtr == 3), getArbitraryCropValue() , Con((nlcd2006 == 82) & (refinement_mtr == 3) & (refine.yxc_dataset > 2011), getArbitraryCropValue()))) 
            print outCon
    
    elif refine.join_operator == 'and':
        output = defineGDBpath(['refine','v2','masks'])+'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'and'+refine.nlcd_years[1]+'_'+refine.name+refine.datarange+'_mask'
        print 'output:', output
        outCon = Con(((nlcd2001 == 82) & (nlcd2 == 82) & (refinement_mtr == 3)), getArbitraryCropValue())
    
    elif refine.join_operator == 'none':
        output = defineGDBpath(['refine','v2','masks'])+refine.traj_dataset+'_nlcd2011'
        print 'output:', output
        outCon = Con(((nlcd2011 == 82) & (refinement_mtr == 3)), getArbitraryCropValue())
    
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



def createMask_dev():
    #reclass the traj_yxc_datarange dataset to the arbitry value of non-crop where pixels are in condtion list
    traj_ytc = defineGDBpath(['refine','v2', 'trajectories'])+'traj_'+refine.yxc_dataset
    print 'traj_ytc:', traj_ytc


    output = defineGDBpath(['refine','v2', 'masks'])+refine.traj_dataset+'_dev122or123or124'
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
    print tablename
    # path to the table you want to import into postgres
    input = defineGDBpath(['refine', 'v2', 'trajectories'])+tablename

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql('v2_'+tablename, engine, schema='refinement')
    
    #add trajectory field to table
    addTrajArrayField('v2_'+tablename, fields, 'refinement')




def createRefinedTrajectory():

    ##### Set environment settings
    arcpy.env.workspace = defineGDBpath(['refine', 'v2', 'masks'])

    ##### loop through each of the cdl rasters and make sure nlcd is last 
    condlist = ['36and61', refine.mask_dev, 'nlcd'+refine.nlcd_ven]

    ##### create a raster list to mosiac together make sure that the intial traj is first in list and nlcd mask is last in the list.
    filelist = [refine.traj_dataset_path]

    for cond in condlist:
        yo = '*'+refine.datarange+'*_'+cond
        print yo 
        for raster in arcpy.ListDatasets('*'+refine.datarange+'*'+cond, "Raster"): 

            print 'raster: ',raster

            filelist.append(raster)

    print 'filelist', filelist
    print 'lenght of list:', len(filelist)

    output = refine.traj_rfnd_dataset
    print "output:", output

    ##### mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, defineGDBpath(['pre', 'v2', 'traj_refined']), output, Raster(refine.traj_dataset_path).spatialReference, '16_BIT_UNSIGNED', refine.res, "1", "LAST","FIRST")



################ Instantiate the class to create yxc object  ########################
refine = ProcessingObject(
      #series
      's13',
      #resolution
      30,
      #data range---i.e. all the cdl years you are referencing 
      [2008,2016],
      #name
      'ytc',
      #join_operator
      'or',
      #devmask
      'dev122to124',
      #nlcd datasets to use in mask
      ['2001','2006','2011']
      )



##################  call functions  ############################################
   
###  create the mtr directly from the trajectories without filtering  ###################----NOTE NEED TO DO THIS TWICE NOW!!!
# createMTR()
# createYearbinaries()

### attach the cld values to the years binaries  #######################
for subtype in refine.subtypelist:
    print subtype
    # attachCDL(subtype)


### create change trajectories for subcategories in yxc ---- OK
# createChangeTrajectories()


###  add traj_yxc[res]_[datarange] attribute to postgres database in the refinement schema ----- ok
# addGDBTable2postgres() 


### create the masks  #####################
# createMask_nlcd()

# createMask_dev()

####call parallel_false script


### create the refined trajectory ########################
###----hard coded still
createRefinedTrajectory()

