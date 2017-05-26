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

'''
Description---
This script is meant to refine the intial trajectory by removing false change from each landcover defined.
'''


#check-out extensions
arcpy.CheckOutExtension("Spatial")

def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/processes/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 


def reclassifyRaster(raster_type,t_lc):
	# Description: reclass cdl rasters based on the specific arc_reclassify_table 

	# Set environment settings
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/reclass/ternary.gdb'

    #change the active directory to the directory containing the cdl's
	dir = 'D:/'+raster_type
	os.chdir(dir)

	#loop through each of the cdl rasters
	for file in glob.glob("*cdls.img"):
		#fnf=file name fragments
		fnf=(os.path.splitext(file)[0]).split("_")

		#get the arc_reclassify table
		inRemapTable = 'C:/Users/bougie/Desktop/gibbs/arc_reclassify_table/'+raster_type+'/'+t_lc

		#define the output
		outRaster = 'rc_'+t_lc+'_'+fnf[0]
		print 'outRaster: ', outRaster

		# Execute Reclassify
		arcpy.gp.ReclassByTable_sa(Raster(file),inRemapTable,"FROM","TO","OUT",outRaster,"NODATA")




def createTraj(degree_lc):
	#combine the reclassified cdl raster to create trajectory for the specific lc of interest

	# Set environment settings
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/reclass/ternary.gdb'
    
    
	nlcd_binary='D:/d/reclass/rc_b_nlcd2011.img'

	# wc='*'+degree_lc+'*'
	#make a list to hold the rasters so can use the list as argument list
	rasterList = [Raster(nlcd_binary),'rc_'+degree_lc+'_2010','rc_'+degree_lc+'_2011','rc_'+degree_lc+'_2012','rc_'+degree_lc+'_2013','rc_'+degree_lc+'_2014','rc_'+degree_lc+'_2015','rc_'+degree_lc+'_2016']
	# for raster in arcpy.ListDatasets(wc, "Raster"): 
	# 	print 'raster: ',raster
	# 	rasterList.append(raster)
    
	print rasterList

	#Execute Combine
	outCombine = Combine(rasterList)
	output = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/traj_'+degree_lc
	#Save the output 
	outCombine.save(output)



def gdbTable2postgres(dataset):
    #set the engine.....
	engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')

	#path to the table you want to import into postgres
	# input = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/traj_'+ dataset
    input = defineGDBpath(['refinement','trajectories'])+'traj_'+ dataset

	#populate the fields from the atribute table into argument variable
	fields = [f.name for f in arcpy.ListFields(input)]

    #convert the gdb tattribute table into a numpy array
	arr = arcpy.da.TableToNumPyArray(input,fields)
	print arr
    
    #convert numpy array into a panda dataframe
	df = pd.DataFrame(data=arr)
	print df

	#import numpy array into postgres
	df.to_sql('traj_'+dataset, engine, schema='refinement')






def PG_DDLandDML(degree_lc):
	#make connection to the postgres database you want to access 
	conn = psycopg2.connect(database = "core", user = "postgres", password = "postgres", host = "localhost", port = "5432")
	print "Opened database successfully"
    
    #define cursor
	cur = conn.cursor()
    
    # add column to table to hold arrays
	cur.execute('ALTER TABLE refinement.traj_' + degree_lc + ' ADD COLUMN traj_array integer[];');
    
    # insert values into array column
	cur.execute('UPDATE refinement.traj_' + degree_lc + ' SET traj_array = ARRAY[rc_b_nlcd2011,rc_' + degree_lc + '_2010,rc_' + degree_lc + '_2011,rc_' + degree_lc + '_2012,rc_' + degree_lc + '_2013,rc_' + degree_lc + '_2014,rc_' + degree_lc + '_2015,rc_' + degree_lc + '_2016];');
    
    #commit the changes
	conn.commit()
	print "Records created successfully";

	#close..........
	conn.close()



def createTrajMask(degree_lc):


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
        print 'in raster: ', raster
        output = raster+'_msk'
        print 'output raster: ', output
        outReclass = Reclassify(raster, "Value", RemapRange(l), "NODATA")
        
        outReclass.save(output)



def mosaicRasters():

	##STILL NEED TO DEVLOP
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb'

	pre_gdb = "C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb"
	traj = "C:/Users/bougie/Desktop/gibbs/production/processes/pre/pre.gdb/traj"

	# Process: Mosaic To New Raster
	arcpy.MosaicToNewRaster_management("C:/Users/bougie/Desktop/gibbs/refinement/trajectories.gdb/traj;traj_q36_msk;traj_t61_msk;traj_tdev_msk", pre_gdb, "traj_refined50", "", "16_BIT_UNSIGNED", "", "1", "LAST", "LAST")


######  call functions  #############################
# reclassifyRaster('cdl','tdev')
# createTraj('tdev')
# gdbTable2postgres('tdev')
# PG_DDLandDML('tdev')
# createTrajMask('tdev')


# mosaicRasters()





def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 



def reclassifyRaster(gdb_args_in, wc, reclass_degree, gdb_args_out):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args_in)

    #loop through each of the cdl rasters
    for raster in arcpy.ListDatasets('*'+wc+'*', "Raster"): 
        
        print 'raster: ',raster

        # outraster = raster.replace("_", "_"+reclasstable+"_")
        outraster = wc + '_' + reclass_degree + raster[-5:]
        print 'outraster: ', outraster
       
        #get the arc_reclassify table
        # inRemapTable = 'C:/Users/Bougie/Desktop/Gibbs/arcgis/arc_reclassify_table/'+reclasstable
        # print 'inRemapTable: ', inRemapTable

        #define the output
        output = defineGDBpath(gdb_args_out)+outraster
        print 'output: ', output

        return_string=getReclassifyValuesString(wc, reclass_degree)

        # Execute Reclassify
        arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")



def getReclassifyValuesString(wc, reclass_degree):
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()

    #DDL: add column to hold arrays
    cur.execute('SELECT value::text,'+reclass_degree+' FROM misc.lookup_'+wc+' WHERE '+reclass_degree+' IS NOT NULL ORDER BY value');
    
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



def createTrajectories(gdb_args_in,wc,gdb_args_out):

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_args)
    
    #get a lsit of all rasters in sepcified database
    rasterList = arcpy.ListDatasets('*'+wc+'*', "Raster")
    
    #sort the rasterlist by accending years
    rasterList.sort(reverse=False)
    
    if wc == 'b':
        #for the binary trajectories, moves the last element in the list (i.e. nlcd_b_2011) to the first element position in the list.
        rasterList.insert(0, rasterList.pop())
    
    print 'rasterList: ',rasterList

    #Execute Combine
    outCombine = Combine(rasterList)
    print 'outCombine: ', outCombine
    
    output = defineGDBpath(gdb_args_out)+'traj_'+wc
    
    #Save the output 
    outCombine.save(output)



def addGDBTable2postgres(gdb_args,tablename,pg_shema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

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






######  call functions  #############################
#-----reclassifyRaster(gdb_args_in, wc, reclass_degree, gdb_args_out)------------------
reclassifyRaster(['ancillary','cdl'], "cdl", "b", ['pre','binaries'])
# reclassifyRaster(['ancillary','misc'], "nlcd", "b", ['pre','binaries'])

#-----use arcgis combine() function to create permuations 
# createTrajectories(['pre','binaries'], "b", ['pre','trajectories'])

#-----add the trajectories atribute table 
# addGDBTable2postgres(['pre','trajectories'],'traj_b','pre')

#-----add field to PG table
# addTrajArrayField('b')

#-----describe
# createReclassifyList('tdev')

#-----describe
#mosaicRasters()