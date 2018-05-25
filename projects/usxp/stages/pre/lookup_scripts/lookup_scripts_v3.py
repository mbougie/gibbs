# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import fnmatch


arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



def getGDBpath(wc):
    for root, dirnames, filenames in os.walk("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\"):
        for dirnames in fnmatch.filter(dirnames, '*{}*.gdb'.format(wc)):
            print dirnames
            gdbmatches = os.path.join(root, dirnames)
    print gdbmatches
    # return json.dumps(gdbmatches)
    return gdbmatches




data = gen.getCurrentInstance()
print data




def labelByMTRqueries():
    ## NOTE: the order that these queries is important!?!?----maybe not because factored in the IS NULL into queries and mtr1 and mtr2 should be fine---mtr5 query needs to last though!!
    cur = conn.cursor()
    res = data['global']['res']
    years=data['global']['years']
    datarange = data['global']['datarange']
    table = data['pre']['traj']['filename']
    lookuptable = data['pre']['traj']['lookup_name']

    #clear columns of values each time run script
    query_initial = 'update pre.{} set mtr = NULL, ytc = NULL, yfc = NULL'.format(lookuptable)
    print query_initial
    cur.execute(query_initial)
    conn.commit()
    
    #run query_mtr1
    query_mtr1 = 'update pre.{} set mtr = 1 where traj_array in (SELECT (SELECT traj_array FROM UNNEST(traj_array) as s HAVING SUM(s) <= 1) from pre.{})'.format(lookuptable,lookuptable)
    print query_mtr1
    cur.execute(query_mtr1)
    conn.commit()

    #run query_mtr2
    query_mtr2 = 'update pre.{} set mtr = 2 where traj_array in (SELECT (SELECT traj_array FROM UNNEST(traj_array) as s HAVING SUM(s) >= {}) from pre.{})'.format(lookuptable, str(len(years)-1), lookuptable)
    print query_mtr2
    cur.execute(query_mtr2)
    conn.commit()
    
    # run the mtr3 and mtr4 queries for all conversion years EXCEPT 2009
    for year in data['global']['years_conv']:
        pre_context = 'cdl'+res+'_b_'+str(year - 2)
        before_year ='cdl'+res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+res+'_b_'+str(year)
        post_context = 'cdl'+res+'_b_'+str(year + 1)
        
        if year != 2009:
            query_mtr3 = 'update pre.{} set mtr = 3, ytc = {} where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where {} = 0 AND {}= 0 AND {} = 1 AND {} = 1 AND ytc IS NULL)'.format(lookuptable, str(year), table, lookuptable, pre_context, before_year, year_cdl, post_context)
            print query_mtr3
            cur.execute(query_mtr3)
            conn.commit()

            query_mtr4 = 'update pre.{} set mtr = 4, yfc = {} where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where {} = 1 AND {}= 1 AND {} = 0 AND {} = 0 AND yfc IS NULL)'.format(lookuptable, str(year), table, lookuptable, pre_context, before_year, year_cdl, post_context)
            print query_mtr4
            cur.execute(query_mtr4)
            conn.commit()

    

    
    ###run 2009 queries AFTER all the other queries (except mtr5) is complete-----------------------------------------------------------------
    query_mtr3 = 'update pre.{} set mtr = 3, ytc = 2009 where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where cdl30_b_2008= 0 AND cdl30_b_2009 = 1 AND cdl30_b_2010 = 1 AND ytc IS NULL AND yfc IS NULL)'.format(lookuptable, table, lookuptable)
    print query_mtr3
    cur.execute(query_mtr3)
    conn.commit()

    query_mtr4 = 'update pre.{} set mtr = 4, yfc = 2009 where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where cdl30_b_2008= 1 AND cdl30_b_2009 = 0 AND cdl30_b_2010 = 0 AND ytc IS NULL AND yfc IS NULL)'.format(lookuptable, table, lookuptable)
    print query_mtr4
    cur.execute(query_mtr4)
    conn.commit()

    ###--------------------------------------------------------------------------------------------------------------------------------------



    ## run query_mtr5 to label all null mtrs AND remove records that dont have mutually exclusive ytc and yfc
    query_mtr5 = 'update pre.{} set mtr = 5, ytc = NULL, yfc = NULL where mtr IS NULL OR (ytc IS NOT NULL AND yfc IS NOT NULL)'.format(lookuptable)
    print query_mtr5
    cur.execute(query_mtr5)
    conn.commit()



#######  update the lookup table  ######
labelByMTRqueries()


