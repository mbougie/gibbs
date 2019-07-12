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


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def importCSV():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	chunksize = 100000
	results = pd.read_csv(filepath_or_buffer='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.csv', chunksize=chunksize)
	print results
		
	# results = pd.read_sql_query(query, con=engine, chunksize=chunksize)
	for df in results:
		print df
		df.to_sql(name='rfs_intensification', con=engine, schema='synthesis', if_exists='append', chunksize=chunksize)





def convertPGtoFC(gdb, pgdb, schema, table, geom_type):
    command = 'ogr2ogr -f "FileGDB" -progress -update D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -nln {3} -nlt {4}'.format(gdb, pgdb, schema, table, geom_type)
    
    os.system(command)

    # gen.alterGeomSRID(pgdb, schema, table, epsg)



def convertFCtoPG(gdb, pgdb, schema, table, out_table):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb -nlt PROMOTE_TO_MULTI -nln {2}.{4} {3} -progress --config PG_USE_COPY YES'.format(gdb, pgdb, schema, table, out_table)
    
    os.system(command)

    # gen.alterGeomSRID(pgdb, schema, table, epsg)






#############################################################################################
###### call functions ##################################################################
#################################################################################################
# convertPGtoFC(gdb='synthesis', pgdb='ksu_v4', schema='synthesis', table='rfs_intensification_t2', geom_type='MultiPolygon')

# convertFCtoPG(gdb='synthesis', pgdb='ksu_v4', schema='synthesis', table='rfs_intensification_pts', out_table='rfs_intensification_pts_agroibis')









convertPGtoFC(gdb='synthesis', pgdb='usxp_deliverables', schema='synthesis', table='extensification_mlra', geom_type='MultiPolygon')





