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


arcpy.CheckOutExtension("Spatial")



#########  global variables   ################
#acccounts for different machines having different cases in path
case=['Bougie','Gibbs']




def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 



def reclassifyRaster(gdb_args, wc, reclasstable):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args)

    #loop through each of the cdl rasters
    for raster in arcpy.ListDatasets(wc, "Raster"): 
        
        print 'raster: ',raster
        outraster = raster.replace("_", "_"+reclasstable+"_")
        # #get the arc_reclassify table
        inRemapTable = 'C:/Users/Bougie/Desktop/Gibbs/arcgis/arc_reclassify_table/'+reclasstable
        print 'inRemapTable: ', inRemapTable

        # #define the output
        output = defineGDBpath(['pre','binaries'])+outraster
        print 'output: ', output

        #Execute Reclassify
        arcpy.gp.ReclassByTable_sa(raster,inRemapTable,"FROM","TO","OUT",output,"NODATA")





def createTrajectories(gdb_args,wc):

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args)
    
    #get a lsit of all rasters in sepcified database
    rasterList = arcpy.ListDatasets('*'+wc+'*', "Raster")
    
    #sort the rasterlist by accending years
    rasterList.sort(reverse=False)
    print 'rasterList: ',rasterList

    #Execute Combine
    outCombine = Combine(rasterList)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(['pre', 'trajectories'])+'traj_'+wc
    #Save the output 
    outCombine.save(output)



def gdbTable2postgres(dataset):
    #set the engine.....
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')

    #path to the table you want to import into postgres
    input = 'C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb/'+dataset

    #populate the fields from the atribute table into argument variable

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]

      
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
     
    df = pd.DataFrame(data=arr)
    # df["rename"] = np.nan


    print df
    
    df.to_sql(dataset, engine, schema='pre')






def PG_DDLandDML(degree_lc):
    conn = psycopg2.connect(database = "core", user = "postgres", password = "postgres", host = "localhost", port = "5432")
    print "Opened database successfully"

    cur = conn.cursor()
    
    # add column to hold arrays
    cur.execute('ALTER TABLE pre.traj ADD COLUMN traj_array integer[];');
    
    # insert values into arrays
    cur.execute('UPDATE pre.traj SET traj_array = ARRAY[rc_' + degree_lc + '_nlcd2,rc_' + degree_lc + '_2010,rc_' + degree_lc + '_2011,rc_' + degree_lc + '_2012,rc_' + degree_lc + '_2013,rc_' + degree_lc + '_2014,rc_' + degree_lc + '_2015,rc_' + degree_lc + '_2016];');

    conn.commit()
    print "Records created successfully";
    conn.close()



def createReclassifyList(degree_lc):


    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/'
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')
    df = pd.read_sql_query("select \"Value\",new_value from refinement.traj_"+degree_lc+" as a JOIN refinement.traj_lookup as b ON a.traj_array = b.traj_array WHERE b.name='"+degree_lc+"'",con=engine)
    
    print df
    a = df.values
    print a
    print type(a)

    l=a.tolist()
    print type(l)
    print l

    for raster in arcpy.ListDatasets('*'+degree_lc, "Raster"): 
        print 'raster', raster
        output = raster+'_msk'

        outReclass = Reclassify(raster, "Value", RemapRange(l), "NODATA")
        
        outReclass.save(output)



def mosaicRasters():
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb'

    pre_gdb = "C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb"
    traj = "C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb/traj"

    # Process: Mosaic To New Raster
    arcpy.MosaicToNewRaster_management("C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/traj;traj_q36_msk;traj_t61_msk;traj_tdev_msk", pre_gdb, "traj_refined50", "", "16_BIT_UNSIGNED", "", "1", "LAST", "LAST")




def mtrAlgorithms(x):
    # arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/pre/pre.gdb'
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')
    df = pd.read_sql_query('select traj_array from pre.traj where mtr IS NULL',con=engine)
    # print 'df--',df



    # a = df.values.flatten()
    # # a.flatten()

    # # print a
    # # print type(a)

    # l=a.tolist()
    # print l

    # for x in l:
    #   print x.count(0)

    for index, row in df.iterrows():
        
        l=row.tolist()
        flat=sum(l, [])
        grouped_l = [(k, sum(1 for i in g)) for k,g in groupby(flat)]
        length = len(grouped_l)
        # print flat
        # print grouped_l
        # print type(length)

        count = flat.count(x)

        
        if x == 1 or x == 2:
            count = flat.count(x-1)
            if count >= 7:
                print count
                print row
                tryit(str(x),str(flat))

        elif x == 3:
            print 'yty'
            if len(grouped_l) == 2 and grouped_l[0][0] == 0 and grouped_l[0][1] > 3:
                print len(grouped_l)
                print grouped_l
                print count
                print row
                tryit(str(x),str(flat))
            # elif len(grouped_l) == 2 and grouped_l[0][0] == 1 and grouped_l[0][1] <= 3:
            #   x=2
            #   tryit(str(x),str(flat))

        elif x == 4:
            print 'ywwwwwwwwwwwty'
            if len(grouped_l) == 2 and grouped_l[0][0] == 1 and grouped_l[0][1] > 3:
                print len(grouped_l)
                print grouped_l
                print count
                print row
                tryit(str(x),str(flat))
                # elif len(grouped_l) == 2 and grouped_l[0][0] == 1 and grouped_l[0][1] <= 3:
                #   x=1
                #   tryit(str(x),str(flat))

        elif x == 5:
            if len(grouped_l) >= 3:
                print count
                print row

                tryit(str(x),str(flat))

   



def tryit(x,row):
    conn = psycopg2.connect(database = "core", user = "postgres", password = "postgres", host = "localhost", port = "5432")
    print "Opened database successfully"

    cur = conn.cursor()
    
    # add column to hold arrays
    # cur.execute('ALTER TABLE pre.traj ADD COLUMN traj_array integer[];');
    
    # insert values into arrays
    cur.execute('UPDATE pre.traj SET mtr = '+x+' where traj_array = array' + row);

    conn.commit()
    print "Records created successfully";
    conn.close()


######  call functions  #############################
#-----reclassifyRaster(geodatabase path arguments, wildcard, arcgis reclass table)------------------
# reclassifyRaster(['ancillary','cdl'], "*", 'cdl_b')
createTrajectories(['pre','binaries'],"b")
# gdbTable2postgres('traj')
# PG_DDLandDML('b')
# createReclassifyList('tdev')
#mosaicRasters()

# trylist = [1,2,3,4,5]
# for x in trylist:
#     mtrAlgorithms(x)
