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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

  
# def execute_task(in_extentDict):
def execute_task(state):
	# in_extentDict = args

	# state = in_extentDict[0]
	print 'state', state

	query = "SELECT ksu_samples.oid, ksu_samples.poly_id, ksu_samples.acres, ksu_samples.dataset, counties_4152.atlas_stco as fips, ksu_samples.st_abbrev, mlra_4152.mlra_id as mlra, wbdhu8_4152.huc8,statsgo_4152.mukey as statsgo, ksu_samples.ssurgo, ksu_samples.mirad, ksu_samples.prism, ksu_samples.cdl_2001, ksu_samples.cdl_2002, ksu_samples.cdl_2003, ksu_samples.cdl_2004, ksu_samples.cdl_2005, ksu_samples.cdl_2006, ksu_samples.cdl_2007, ksu_samples.cdl_2008, ksu_samples.cdl_2009, ksu_samples.cdl_2010, ksu_samples.cdl_2011, ksu_samples.cdl_2012, ksu_samples.cdl_2013, ksu_samples.cdl_2014, ksu_samples.cdl_2015, ksu_samples.cdl_2016, ksu_samples.lon, ksu_samples.lat FROM merged.ksu_samples,attributes.counties_4152,attributes.mlra_4152,attributes.statsgo_4152,attributes.wbdhu8_4152 WHERE ST_WITHIN(ksu_samples.geom, counties_4152.geom) AND ST_WITHIN(ksu_samples.geom, mlra_4152.geom) AND ST_WITHIN(ksu_samples.geom, statsgo_4152.geom) AND ST_WITHIN(ksu_samples.geom, wbdhu8_4152.geom) AND ksu_samples.st_abbrev = '{}' ".format(state)

	# query = query.translate(None, '\t\n')
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu_v3')
	chunksize = 100000
	results = pd.read_sql_query(query, con=engine, chunksize=chunksize)
	for df in results:

		print df
		# df.to_csv('E:\\ksu\\csv\\{}.csv'.format(state), index=False, header=True,  chunksize=5000, mode='a')

		df.to_sql(state, con=engine, schema='states', chunksize=chunksize)






def run():
	print 'state_6---------------------------------------------------------------------------------------------------------------------------------------'
	fishnet = 'states_6'

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["st_abbrev","SHAPE@"]):


		state = row[0]
		
		execute_task(state)




if __name__ == '__main__':
	run()