import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import webcolors as wc
import palettable
# import matplotlib as mpl

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
# import replace_61_w_hard_crop


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def addGDBTable2postgres(inraster):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields('D:\\projects\\usxp\\deliverables\\maps\\slope\\slope.gdb\\{}'.format(inraster))]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray('D:\\projects\\usxp\\deliverables\\maps\\slope\\slope.gdb\\{}'.format(inraster),fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    # total=df['count'].sum()

    # print df.describe() 

    # hist = df.hist(bins=3)
    
    # # # use pandas method to import table into psotgres
    df.to_sql(inraster, engine, schema='slope')
    
    # # #add trajectory field to table
    # addAcresField('counts_yxc', data['post'][yxc]['filename'], yxc, 30, total)




def appyValuesToMTR(schema, inraster, reclassed_raster):
	arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(schema)

	filename = 'gssurgo_{}_30m'.format(inraster)
	print 'filename:', filename

	cond = "Value <> 3" 
	outraster = SetNull('D:\\projects\\usxp\\deliverables\\s35\\s35.gdb\\s35_mtr', filename, cond)

	outraster.save(reclassed_raster)

	gen.buildPyramids(reclassed_raster)




def blockStats(schema, inraster, cellsize):
	arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(schema)
	
	nbr = NbrRectangle(cellsize, cellsize, "CELL")
	outBlockStat = BlockStatistics(inraster, nbr, "MAJORITY", "DATA")
	outAggreg = Aggregate(outBlockStat, cellsize, "MAXIMUM", "TRUNCATE", "DATA")
	outBlockStat=None

	output_raster = "{}_bs3km".format(inraster)
	outAggreg.save(output_raster)
	gen.buildPyramids(output_raster)




if __name__ == '__main__':
	print ('----------------this is the main function----------------------------------')

	######  define parameters  ###################################
	schema='slope'
	inraster='slopegradwta'
	reclassed_raster='s35_slope_null'


	## setnull function to reclass mtr3 with nicc values!!!
	# appyValuesToMTR(schema, inraster, reclassed_raster)

	### setblockstats!!!
	# blockStats(schema, reclassed_raster, 100)



	### export raster attribute table to postgres
	addGDBTable2postgres(reclassed_raster)



