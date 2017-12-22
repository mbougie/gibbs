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
rootpath = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = '{}{}/{}/{}.gdb/'.format(rootpath,arg_list[0],arg_list[1],arg_list[2])
    # print 'gdb path: ', gdb_path 
    return gdb_path

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


def getSeries():


    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = "SELECT * FROM series.params inner join series.core using(series) where params.series='s15';"
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    # print df
    for index, row in df.iterrows():
        # print row
        return row
    


returned_row = getSeries()
core = returned_row['n1']
# print data

###convert unicide to string datatype
# core = { str(key):str(value) for key,value in data.items() }
print core['filter']
print type(core['filter'])

# def foo():
#     print 'gfgfgf'


#################### class to create core object  ####################################################
# class ProcessingObject(object):
#     def __init__(self, core['series'], route, res, mmu, years, filter_gdb, filter_key):
#         self.core['series'] = core['series']
#         self.res = str(res)
#         self.years = years
#         self.filter_key = filter_key
#         self.filter_gdb = filter_gdb
#         self.mmu = mmu
#         self.route = route




#         self.datarange = str(self.years[0])+'to'+str(self.years[1])
#         print 'self.datarange', self.datarange
#         self.traj_name = self.series+"_traj_cdl"+self.res+"_b_"+self.datarange+"_rfnd"
#         self.traj_path = defineGDBpath(['pre','v2','traj_refined'])+self.traj_name

#         self.filter_parentnode = self.getFilterParentNode()

#     def getFilterParentNode(self):
#         if self.route == 'r2' or self.route == 'r3':
#             return self.traj_name

                



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
    print 'createMTR()----------------------------------------------------------------------'
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    # raster = defineGDBpath(['sa','mmu']) + core.traj_name+"_n8h"
    raster = defineGDBpath(['s15','core','core'])+"s13_traj_cdl30_b_2008to2016_rfnd_n4h"
    print 'raster: ', raster

    output = defineGDBpath(['s15','core','core'])+"s13_traj_cdl30_b_2008to2016_rfnd_n4h_mtr"
    print 'output:', output

    # reclassArray = createReclassifyList() 

    # outReclass = Reclassify(Raster(raster), "Value", RemapRange(reclassArray), "NODATA")
    
    # outReclass.save(output)

    # gen.buildPyramids(output)



def createReclassifyList():
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = 'SELECT "Value", mtr from pre.v2_traj_cdl' + core.res + '_b_' + core.datarange + ' as a JOIN pre.traj_' + core.datarange + '_lookup as b ON a.traj_array = b.traj_array'
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
    filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'nh8':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    print 'filter_combo----', filter_combos[core['filter']]
    
    # parent_raster = core.traj_path
    # print 'parent raster', parent_raster
    # output = defineGDBpath(core.filter_gdb)+core.filter_parentnode+'_'+core.filter_key
    # print 'output: ',output

    # ## check if dataset already exists
    # if arcpy.Exists(output):
    #     print 'dataset already exists'
    #     return
    

    # else:
    #     print 'creating new filter dataset...............................'
    #     #Execute MajorityFilter
    #     outMajFilt = MajorityFilter(core.traj_path, filter_combos[core.filter_key][0], filter_combos[core.filter_key][1])
        
    #     #save processed raster to new file
    #     outMajFilt.save(output)

    #     gen.buildPyramids(output)



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





def addGDBTable2postgres():
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(['post','ytc'])
    
    # wc = '*'+core.res+'*'+core.datarange+'*'+core.filter+'*_msk5_nbl'
    wc = 'ytc30_2008to2016_initial'
    print wc


    for raster in arcpy.ListDatasets(wc, "Raster"): 

    # for table in arcpy.ListTables(wc): 
        print 'raster: ', raster
        
        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(raster)]
        print fields

        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(raster,fields)
        print arr

        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        # use pandas method to import table into psotgres
        df.to_sql(raster, engine, schema='counts')

        #add trajectory field to table
        addAcresField(raster, 'counts')





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



def AddNullValuesToRaster():
    in_conditional_raster = defineGDBpath(['core','mmu'])+'traj_cdl30_b_2008to2016_rfnd_s9_n8h_mtr_8w_msk5'
    in_true_raster_or_constant = defineGDBpath(['core','mtr'])+'traj_cdl30_b_2008to2016_rfnd_s9_n8h_mtr'
    


    outraster = defineGDBpath(['core','mmu'])+'traj_cdl30_b_2008to2016_rfnd_s9_n8h_mtr_8w_msk5_hybrid'

    cond = "Value = 1"

    OutRas = Con(in_conditional_raster, in_true_raster_or_constant, in_conditional_raster, cond)

    OutRas.save(outraster)






#### COMMENTS ##########################################

#could the addGDBTable2postgres() function be added to a qaqc script or should it stay in this script??


# AddNullValuesToRaster()


def bar():
    print 'ddgfdffqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq'

dispatcher = {'foobar': [createMTR, majorityFilter]}

def fire_all(func_list):
    for f in func_list:
        f()

fire_all(dispatcher['foobar'])
