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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import json



'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
# case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template




data = getJSONfile()
print data



def createMTR():
    print 'createMTR()----------------------------------------------------------------------'
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    arcpy.env.workspace = data['core']['gdb']

    raster_in = data['core']['function']['createMTR']['input']
    print 'raster_in', raster_in
    raster_out = data['core']['function']['createMTR']['output']
    print 'raster_out: ',raster_out

    reclassArray = createReclassifyList() 

    outReclass = Reclassify(Raster(raster_in), "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(raster_out)

    gen.buildPyramids(raster_out)



def createReclassifyList():
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = 'SELECT "Value", mtr from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array'.format(data['pre']['traj']['filename'], data['pre']['traj']['lookup'])
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
    arcpy.env.workspace = data['core']['gdb']

    filter_key = data['core']['filter']
    print 'filter_key', filter_key
    
    filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    print 'filter_combo----', filter_combos[filter_key]
    
    
    raster_in = data['core']['function']['majorityFilter']['input']
    print 'raster_in', raster_in
    raster_out = data['core']['function']['majorityFilter']['output']
    print 'raster_out: ',raster_out

    print 'creating new filter dataset...............................'
    ##Execute MajorityFilter
    outMajFilt = MajorityFilter(raster_in, filter_combos[filter_key][0], filter_combos[filter_key][1])
    
    ##save processed raster to new file
    outMajFilt.save(raster_out)

    gen.buildPyramids(raster_out)



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



def runRoutes():
    if data['core']['route'] == 'r1':
        majorityFilter()
        createMTR()
    elif data['core']['route'] == 'r2':
        majorityFilter()
        createMTR()
    elif data['core']['route'] == 'r3':
        majorityFilter()
        # mmu()
        # createMTR()
        


#### COMMENTS ##########################################

#could the addGDBTable2postgres() function be added to a qaqc script or should it stay in this script??


# AddNullValuesToRaster()


# def bar():
#     print 'ddgfdffqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq'

# dispatcher = {'foobar': [createMTR, majorityFilter]}

# def fire_all(func_list):
#     for f in func_list:
#         f()

# fire_all(dispatcher['foobar'])


runRoutes()
