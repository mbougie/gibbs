import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
import json
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager


sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import import_pandas_io as pandas_io



#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 






def convertPGtoFC(gdb, pgdb, schema, table, geom_type):
    command = 'ogr2ogr -f "FileGDB" -progress -update {0} PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -nln {3} -nlt {4}'.format(gdb, pgdb, schema, table, geom_type)
    
    os.system(command)

    # gen.alterGeomSRID(pgdb, schema, table, epsg)



def convertFCtoPG(gdb, pgdb, schema, table, out_table):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nlt PROMOTE_TO_MULTI -nln {2}.{4} {3} -progress --config PG_USE_COPY YES'.format(gdb, pgdb, schema, table, out_table)
    
    os.system(command)

    # gen.alterGeomSRID(pgdb, schema, table, epsg)



def importCSV():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	# df = pd.read_csv('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\seth\\rfs_intensification_n2o.csv')


	# df.to_sql(name='rfs_intensification_n2o', con=engine, schema='synthesis_intensification')





def importCSV(csv, schema, table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	chunksize = 100000
	results = pd.read_csv(csv, chunksize=chunksize)
	df = pd.read_csv(filepath_or_buffer=csv)

	print df
	
	for df in results:
		print df
		df.to_sql(name=table, con=engine, schema=schema, if_exists='append', chunksize=chunksize)




def inportCSV_t2(file, schema, table):


	conn = psycopg2.connect("host=144.92.235.105 dbname=synthesis user=mbougie password=Mend0ta!")
	cur = conn.cursor()
	# with open('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\seth\\rfs_intensification_n2o.csv', 'r') as f:

	# 	# Notice that we don't need the `csv` module.
	# 	next(f)  # Skip the header row.
	# 	cur.copy_from(f, table='synthesis_intensification.rfs_intensification_n2o', sep=',')
	f = open(file, 'r')

	cur.copy_from(f, '{0}.{1}'.format(schema, table), sep=',')

	f.close()

	conn.commit()

#############################################################################################
###### call functions ##################################################################
#################################################################################################

###steps#######
#### import rfs_intensification.csv dataset into postgres.  This dataset has the attached values to the point dataset I sent KSU
pandas_io.pandasCSV_io(pgdb='synthesis', file='H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\schemas\\ksu\\rfs_intensification.csv', schema='intensification_ksu', table='rfs_intensification')


# convertFCtoPG(gdb='D:\\projects\\synthesis\\s35\\intensification\\v_3\\synthesis_intensification.gdb', pgdb='synthesis', schema='intensification_agroibis', table='rfs_intensification_v3_agroibis', out_table='rfs_intensification_v3_agroibis')
# convertFCtoPG(gdb='D:\\data\\mlra\\mlra.gdb', pgdb='usxp_deliverables', schema='spatial', table='mlra_5070_dissolved', out_table='mlra_5070_dissolved')
# convertFCtoPG(gdb='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\nathan\\maps_created_for_nathan\\nathan_maps.gdb', pgdb='usxp_deliverables', schema='synthesis_intensification', table='nathan_mlra', out_table='nathan_mlra')

# convertPGtoFC(gdb='D:\\projects\\synthesis\\s35\\intensification\\v_3\\synthesis_intensification.gdb', pgdb='synthesis', schema='intensification_agroibis', table='rfs_intensification_results_counties', geom_type='MultiPolygon')
# convertPGtoFC(gdb='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis_extensification.gdb', pgdb='usxp_deliverables', schema='synthesis_extensification', table='agroibis', geom_type='MultiPolygon')
# convertPGtoFC(gdb='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis_extensification.gdb', pgdb='usxp_deliverables', schema='synthesis_extensification', table='carbon_rfs_counties_v2', geom_type='MultiPolygon')
# convertPGtoFC(gdb='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis.gdb', pgdb='usxp_deliverables', schema='synthesis_intensification', table='nathan_mlra', geom_type='MultiPolygon')
# inportCSV_t2()


# importCSV(csv='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\nathan\\maps_created_for_nathan\\rfs_corn_impact_mlra.csv', schema='synthesis_intensification', table='rfs_corn_impact_mlra')




# convertFCtoPG(gdb='D:\\projects\\synthesis\\s35\\intensification\\v_3\\synthesis_intensification.gdb', pgdb='synthesis', schema='intensification_agroibis', table='rfs_intensification_v3_agroibis', out_table='rfs_intensification_v3_agroibis')





