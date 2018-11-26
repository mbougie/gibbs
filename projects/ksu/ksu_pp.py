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
def execute_task(args):
	in_extentDict = args

	state = in_extentDict[0]
	print 'state', state

	query = """SELECT 
			  ksu_samples_final.unique_id, 
			  ksu_samples_final.poly_id, 
			  ksu_samples_final.acres, 
			  ksu_samples_final.dataset, 
			  ksu_samples_final.fips, 
			  ksu_samples_final.st_abbrev, 
			  ksu_samples_final.mlra, 
			  ksu_samples_final.huc8, 
			  ksu_samples_final.statsgo, 
			  ksu_samples_final.ssurgo, 
			  ksu_samples_final.mirad, 
			  ksu_samples_final.prism, 
			  ksu_samples_final.cdl_2001, 
			  ksu_samples_final.cdl_2002, 
			  ksu_samples_final.cdl_2003, 
			  ksu_samples_final.cdl_2004, 
			  ksu_samples_final.cdl_2005, 
			  ksu_samples_final.cdl_2006, 
			  ksu_samples_final.cdl_2007, 
			  ksu_samples_final.cdl_2008, 
			  ksu_samples_final.cdl_2009, 
			  ksu_samples_final.cdl_2010, 
			  ksu_samples_final.cdl_2011, 
			  ksu_samples_final.cdl_2012, 
			  ksu_samples_final.cdl_2013, 
			  ksu_samples_final.cdl_2014, 
			  ksu_samples_final.cdl_2015, 
			  ksu_samples_final.cdl_2016, 
			  ksu_samples_final.lon, 
			  ksu_samples_final.lat FROM merged.ksu_samples_final WHERE ksu_samples_final.st_abbrev = '{}' """.format(state)
	
	print query
	query = query.translate(None, '\t\n')
	print query


	# query = query.translate(None, '\t\n')
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu_v4')

	df = pd.read_sql_query(query, con=engine)
	print df

	df.to_csv('E:\\ksu\\v4\\csv\\{}.csv'.format(state), index=False, header=True,  chunksize=100000, mode='a')





def getSQL(state):
	##garbage remove when I can!!!!!

	query = query = """ SELECT 
				ksu_samples.oid,
				ksu_samples.poly_id, 
				ksu_samples.area_m2, 
				ksu_samples.acres_entire, 
				ksu_samples.acres, 
				ksu_samples.dataset,
				counties_4152.atlas_stco as fips, 
				mlra_4152.mlra_id as mlra,
				wbdhu8_4152.huc8,
				statsgo_4152.mukey as statsgo,
				ksu_samples.ssurgo,
				ksu_samples.mirad, 
				ksu_samples.prism,  
				ksu_samples.cdl_2001, 
				ksu_samples.cdl_2002, 
				ksu_samples.cdl_2003, 
				ksu_samples.cdl_2004, 
				ksu_samples.cdl_2005, 
				ksu_samples.cdl_2006, 
				ksu_samples.cdl_2007, 
				ksu_samples.cdl_2008, 
				ksu_samples.cdl_2009, 
				ksu_samples.cdl_2010, 
				ksu_samples.cdl_2011, 
				ksu_samples.cdl_2012, 
				ksu_samples.cdl_2013, 
				ksu_samples.cdl_2014, 
				ksu_samples.cdl_2015, 
				ksu_samples.cdl_2016, 
				ksu_samples.lon, 
				ksu_samples.lat, 
				ksu_samples.atlas_st, 
				ksu_samples.st_abbrev    
			FROM 
				merged.ksu_samples,
				attributes.counties_4152,
				attributes.mlra_4152,
				attributes.statsgo_4152,
				attributes.wbdhu8_4152 WHERE 
				ST_WITHIN(ksu_samples.geom, counties_4152.geom) AND 
				ST_WITHIN(ksu_samples.geom, mlra_4152.geom) AND 
				ST_WITHIN(ksu_samples.geom, statsgo_4152.geom) AND 
				ST_WITHIN(ksu_samples.geom, wbdhu8_4152.geom) AND ksu_samples.st_abbrev = '{0}' LIMIT 10""".format(state)
	

	query = query.translate(None, '\t\n')
	print query
	return query






def run():
	print 'hi'
	# conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")

	##NOTE: for arcgis NEED to subset tiles because empty tiles dont work.  FOr numpy processing it can deal with empty tiles!!!
	fishnet = 'states'

	extDict = {}

	for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["st_abbrev","SHAPE@"]):

		st_abbrev = row[0]
		# if st_abbrev == 'AZ':
		print st_abbrev
		# extent_curr = row[1].extent
		ls = []
		ls.append(getSQL(st_abbrev))
		# ls.append(extent_curr.YMin)
		# ls.append(extent_curr.XMax)
		# ls.append(extent_curr.YMax)
		extDict[st_abbrev] = ls

	# print 'extDict', extDict
	# print'extDict.items',  extDict.items()
    

	# #######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=9)
	pool.map(execute_task, [(ed) for ed in extDict.items()])
	pool.close()
	pool.join




if __name__ == '__main__':
	run()