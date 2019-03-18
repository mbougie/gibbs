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



def csv2pg():
	['WI', 'samples_WI']

	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu_v4')

	chunksize = 100000
	results = pd.read_csv(filepath_or_buffer='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.csv', chunksize=chunksize)
	# results = pd.read_csv(filepath_or_buffer='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.csv', nrows=1000000)
	print results


	# results.to_sql(name='rfs_intensification_1000000', con=engine, schema='synthesis')
		
	for df in results:
		print df
		df.columns = map(str.lower, df.columns)
		df.to_sql(name='rfs_intensification_t2', con=engine, schema='synthesis', if_exists='append', chunksize=chunksize)




def convertPGtoFC():
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.gdb PG:"dbname=ksu_v4 user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM synthesis.rfs_intensification_poly" -nln rfs_intensification_poly -nlt MULTIPOLYGON'
    print command
    os.system(command)


def convertFCtoPG():
	command = 'ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v4 user=mbougie host=144.92.235.105 password=Mend0ta!" D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.gdb -nlt PROMOTE_TO_MULTI -nln synthesis.rfs_intensification_agroibis rfs_intensification_agroibis -progress --config PG_USE_COPY YES'

	os.system(command)

	# gen.alterGeomSRID(pgdb, schema, table, epsg)






#####call functions
# csv2pg()
# convertPGtoFC()
convertFCtoPG()