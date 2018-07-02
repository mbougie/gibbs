import sys
import os
#import modules from other folders
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import arcpy
from arcpy import env
from arcpy.sa import *
import glob

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
# import general as gen
import json
import fnmatch
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import multiprocessing




arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

print 'this is'
cur = conn.cursor()

arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 


arcpy.env.workspace = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/ancillary/vector/shapefiles.gdb/'



def getZonalinfo():	
	# Use the ListFeatureClasses function to return a list of shapefiles.
	fc = 'states'
    
	zonelist = []
	cursor = arcpy.da.SearchCursor(fc, ['atlas_st'])
	for row in cursor:
		zonelist.append(row[0])
	return zonelist



def applyAPI():
	df_list = []
	for state in getZonalinfo():
		print 'state', state
		
		createStateByTilelist(getTilelistByState(state), state)

		

def getTilelistByState(state):

	query = "SELECT objectid FROM test_yo.zonal_hist_states_and_tiles_t3 WHERE atlas_{} <> 0".format(state)
	cur.execute(query)
	the_tuple = cur.fetchall()
	print the_tuple
	return the_tuple





def createStateByTilelist(the_tuple, state):
	querylist = []
	for tup in the_tuple:
		print tup[0]
		query = """SELECT tile,traj,state,ytc,traj_array,mask,count(traj) FROM test_yo.tile_{} GROUP BY tile,traj,state,ytc,traj_array,mask""".format(str(tup[0]))
		querylist.append(query)
	

	str1 = ' UNION '.join(querylist)
	

	meow = 'Create Table hiyo.st_{} as SELECT tile,traj,state,ytc,traj_array,mask,count(traj) FROM ({}) as uniontable GROUP BY tile,traj,state,ytc,traj_array,mask'.format(state, str1)




	print 'meow------------------------', meow

	# cur.execute(str1);

	# conn.commit()
	# print "Records created successfully";









# applyAPI()


