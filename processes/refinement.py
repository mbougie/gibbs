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
createTraj('tdev')
gdbTable2postgres('tdev')
PG_DDLandDML('tdev')
createTrajMask('tdev')


# mosaicRasters()
