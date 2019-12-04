import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import numpy as np
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


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def importCSV(csv, pgdb, schema, table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{0}'.format(pgdb))


	# chunksize = 100000
	# results = pd.read_csv(filepath_or_buffer='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.csv', chunksize=chunksize)
	df = pd.read_csv(filepath_or_buffer=csv)

	print df
	
	df.to_sql(name=table, con=engine, schema=schema)
	# for df in results:
	# 	print df
	# 	df.to_sql(name='rfs_intensification', con=engine, schema='synthesis', if_exists='append', chunksize=chunksize)




#### import csv datasets to extensification_seth schema in postgres ################

def importExtensification():
	wd = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\external_data\\seth\\fips summaries\\fips summaries\\net"
	for csv in os.listdir(wd):
		csv_path = '{0}//{1}'.format(wd,csv)
		importCSV(csv=csv_path, pgdb='synthesis', schema='extensification_seth', table=(csv.split('.'))[0])



def importIntensification():
	path = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\schemas\\seth\\fips_summaries (11-20-2019)\\fips_summaries (11-20-2019)"

	for root,d_names,f_names in os.walk(path):

		for csv in f_names:
			csv_path = '{0}//{1}'.format(root,csv)
			importCSV(csv=csv_path, pgdb='synthesis', schema='intensification_11_20_2019', table=(csv.split('.'))[0])







###### call function #######################################
# importExtensification()
importIntensification()
