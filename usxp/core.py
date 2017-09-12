from sqlalchemy import create_engine
import numpy as np, sys, os
import pandas as pd
# import collections
# from collections import namedtuple
import arcpy
from arcpy import env
from arcpy.sa import *
# import glob
import psycopg2
import general as gen



'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"

###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

#################### class to create core object  ####################################################
class CoreObject:

    def __init__(self, res, years, filter, mmu):
        self.res = res
        self.years = years

        if self.years[1] == 2016:
            self.datarange = str(self.years[0])+'to'+str(self.years[1]-1)
            print 'self.datarange:', self.datarange
            
        else:

            self.datarange = str(self.years[0])+'to'+str(self.years[1])
            print 'self.datarange:', self.datarange


        self.traj_name = "traj_cdl"+self.res+"_b_"+self.datarange+"_rfnd"
        self.traj_path = defineGDBpath(['pre','trajectories'])+self.traj_name
        self.filter = filter
        self.wc = "*"+res+"*"+self.datarange+"*"
        self.mmu = mmu



def addColorMap(inraster,template):
    ##Add Colormap
    ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

    try:
        import arcpy
        # arcpy.env.workspace = r'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'
        
        ##Assign colormap using template image
        arcpy.AddColormap_management(inraster, "#", template)
        

    except:
        print "Add Colormap example failed."
        print arcpy.GetMessages()



def createMTR():
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    arcpy.env.workspace = defineGDBpath(['core','filter'])


    for raster in arcpy.ListDatasets(core.wc, "Raster"): 

        print 'raster: ', raster

        output = defineGDBpath(['core','mtr'])+raster+'_mtr'
        print 'output:', output

        reclassArray = createReclassifyList() 

        outReclass = Reclassify(Raster(raster), "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        gen.buildPyramids(output)



def createReclassifyList():
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = 'SELECT "Value", mtr from pre.traj_cdl' + core.res + '_b_' + core.datarange + ' as a JOIN pre.traj_cdl' + core.res + '_b_' + core.datarange + '_lookup as b ON a.traj_array = b.traj_array'
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



def majorityFilter():
    #filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    filter_combos = {'n8h':["EIGHT", "HALF"]}
    for k, v in filter_combos.iteritems():
        print k,v

        output = defineGDBpath(['core','filter'])+core.traj_name+'_'+k
        print 'output: ',output

        # Execute MajorityFilter
        outMajFilt = MajorityFilter(core.traj_path, v[0], v[1])
        
        #save processed raster to new file
        outMajFilt.save(output)

        gen.buildPyramids(output)



def focalStats(gdb_args_in, dataset, gdb_args_out):
    # arcpy.CheckOutExtension("Spatial")
    # arcpy.env.workspace = defineGDBpath(gdb_args_in)

    # filter_combos = {'k3':[3, 3, "CELL"],'k5':[5, 5, "CELL"]}
    filter_combos = {'k3':[3, 3, "CELL"]}
    for k, v in filter_combos.iteritems():
        print k,v
        # for raster in arcpy.ListDatasets(dataset, "Raster"): 
        raster_in = defineGDBpath(gdb_args_in) + dataset
        print 'raster_in: ', raster_in

        output = defineGDBpath(gdb_args_out)+dataset+'_'+k
        print 'output: ',output

        neighborhood = NbrRectangle(v[0], v[1], v[2])

        # Execute FocalStatistics
        outFocalStatistics = FocalStatistics(Raster(raster_in), neighborhood, "MAJORITY")

        outFocalStatistics.save(output)
        
        gen.buildPyramids(output)




####################  mmu functions  ##########################################

def regionGroup():
    #define workspace
    arcpy.env.workspace=defineGDBpath(['core', 'mtr'])

    for raster in arcpy.ListDatasets(core.wc, "Raster"): 

        print 'raster: ', raster

        filter_combos = {'8w':["EIGHT", "WITHIN"]}
        for k, v in filter_combos.iteritems():
            print k,v

            output=defineGDBpath(['core','mmu'])+raster+'_'+k
            print 'output: ',output

            # Execute RegionGroup
            outRegionGrp = RegionGroup(Raster(raster), v[0], v[1],"NO_LINK")

            # Save the output 
            print 'save the output'
            outRegionGrp.save(output)

            gen.buildPyramids(output)






def clipByMMUmask():
    #define workspace
    arcpy.env.workspace=defineGDBpath(['core', 'mmu'])

    for raster in arcpy.ListDatasets(core.wc, "Raster"): 

        print 'raster: ', raster

        # for count in masks_list:
        cond = "Count < " + str(gen.getPixelCount(core.res, core.mmu))
        print 'cond: ',cond

        output = raster+'_msk'+ str(core.mmu)

        print output

        outSetNull = SetNull(raster, 1, cond)

        # Save the output 
        outSetNull.save(output)





def addGDBTable2postgres(gdb_args,wc,pg_shema):
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)

    for table in arcpy.ListTables(wc): 
        print 'table: ', table

        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(table)]
        print fields

        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(table,fields)
        print arr

        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        # use pandas method to import table into psotgres
        df.to_sql(table, engine, schema=pg_shema)

        #add trajectory field to table
        addAcresField(table, pg_shema)





def addAcresField(tablename, schema):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN acres bigint;');
    
    #DML: insert values into new array column
    cur.execute('UPDATE '+ schema + '.' + tablename + ' SET acres = count * ' + gen.getPixelConversion2Acres(core.res));
    
    conn.commit()
    print "Records created successfully";
    conn.close()


















################ Instantiate the class to create core object  ########################
core = CoreObject(
      #resolution
      '30',
      #data range---i.e. all the cdl years you are referencing 
      [2010,2016],
      #filter used
      'n8h',
      #mmu
       5
      )



#############################  Call Functions ######################################
##------filter gdb--------------
# majorityFilter()

##------mtr gdb-----------------
# createMTR()

##------mmu gdb-----------------
# regionGroup()
clipByMMUmask()

##find way to call the parrell_nibble function here

##########  NEW     ###################################
# addGDBTable2postgres(['core_2008to2012','mmu'],'*30*nbl_table','counts')





