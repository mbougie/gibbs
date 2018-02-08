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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
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





data = gen.getJSONfile()
print data















def labelByMTR():
    cur = conn.cursor()
    res = data['global']['res']
    datarange = data['global']['datarange']
    table = data['pre']['traj']['filename']
    lookuptable = data['core']['lookup']

    #clear columns of values each time run script
    query_initial = 'update pre.{} set mtr = NULL, ytc = NULL, yfc = NULL'.format(lookuptable)
    print query_initial
    cur.execute(query_initial)
    conn.commit()
    
    query_mtr1 = 'update pre.{} set mtr = 1 where traj_array in (SELECT (SELECT traj_array FROM UNNEST(traj_array) as s HAVING SUM(s) <= 1) from pre.{})'.format(lookuptable,lookuptable)
    print query_mtr1
    cur.execute(query_mtr1)
    conn.commit()

    query_mtr2 = 'update pre.{} set mtr = 2 where traj_array in (SELECT (SELECT traj_array FROM UNNEST(traj_array) as s HAVING SUM(s) >= 9) from pre.{})'.format(lookuptable,lookuptable)
    print query_mtr2
    cur.execute(query_mtr2)
    conn.commit()
    

    for year in data['global']['years_conv']:
        pre_context = 'cdl'+res+'_b_'+str(year - 2)
        before_year ='cdl'+res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+res+'_b_'+str(year)
        post_context = 'cdl'+res+'_b_'+str(year + 1)
        
        # run the mtr3 and mtr4 queries for all conversion years except 2009
        if year != 2009:
            query_mtr3 = 'update pre.{} set mtr = 3, ytc = {} where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where {} = 0 AND {}= 0 AND {} = 1 AND {} = 1 AND ytc IS NULL)'.format(lookuptable, str(year), table, lookuptable, pre_context, before_year, year_cdl, post_context)
            print query_mtr3
            cur.execute(query_mtr3)
            conn.commit()

            query_mtr4 = 'update pre.{} set mtr = 4, yfc = {} where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where {} = 1 AND {}= 1 AND {} = 0 AND {} = 0 AND yfc IS NULL)'.format(lookuptable, str(year), table, lookuptable, pre_context, before_year, year_cdl, post_context)
            print query_mtr4
            cur.execute(query_mtr4)
            conn.commit()

    

    
    ###run 2009 queries after ALL the other queries except mtr5 is complete
    query_mtr3 = 'update pre.{} set mtr = 3, ytc = 2009 where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where cdl30_b_2008= 0 AND cdl30_b_2009 = 1 AND cdl30_b_2010 = 1 AND ytc IS NULL AND yfc IS NULL)'.format(lookuptable, table, lookuptable)
    print query_mtr3
    cur.execute(query_mtr3)
    conn.commit()

    query_mtr4 = 'update pre.{} set mtr = 4, yfc = 2009 where traj_array in (SELECT traj_array FROM pre.{} a INNER JOIN pre.{} b using(traj_array) Where cdl30_b_2008= 1 AND cdl30_b_2009 = 0 AND cdl30_b_2010 = 0 AND ytc IS NULL AND yfc IS NULL)'.format(lookuptable, table, lookuptable)
    print query_mtr4
    cur.execute(query_mtr4)
    conn.commit()

    ## this is the "clean-up" query
    query_mtr5 = 'update pre.{} set mtr = 5, ytc = NULL, yfc = NULL where mtr IS NULL OR (ytc IS NOT NULL AND yfc IS NOT NULL)'.format(lookuptable)
    print query_mtr5
    cur.execute(query_mtr5)
    conn.commit()








def FindRedundantTrajectories():
    # what is the purpose of this function??
    cur = conn.cursor()
    res = data['global']['res']
    datarange = data['global']['datarange']
    table = data['pre']['traj']['filename']
    lookuptable = data['core']['lookup']
    # table = 'pre.traj_cdl{0}_b_{1}'.format(pre.res, pre.datarange)
    # lookuptable = 'pre.traj_{}_lookup'.format(pre.datarange)

    query_list = []
    for year in data['global']['years_conv']:
        pre_context = 'cdl'+res+'_b_'+str(year - 2)
        before_year ='cdl'+res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+res+'_b_'+str(year)
        post_context = 'cdl'+res+'_b_'+str(year + 1)
        query_ytc = 'SELECT traj_array FROM pre.'+table+' a INNER JOIN pre.'+lookuptable+'_v3 b using(traj_array) Where '+pre_context+' = 0 AND '+before_year+'= 0 AND '+year_cdl+' = 1 AND '+post_context+' = 1'
        print query_ytc
        query_list.append(query_ytc)

        query_yfc = 'SELECT traj_array FROM pre.'+table+' a INNER JOIN pre.'+lookuptable+'_v3 b using(traj_array) Where '+pre_context+' = 1 AND '+before_year+'= 1 AND '+year_cdl+' = 0 AND '+post_context+' = 0'
        print query_yfc
        query_list.append(query_yfc)



    print query_list
    str1 = ' UNION ALL '.join(query_list)
    print str1


    query1 = 'update pre.{} set mtr = 5, ytc = NULL where traj_array in (select * from ('+str1+') ou where (select count(*) from ('+str1+') inr where inr.traj_array = ou.traj_array) > 1)'.format(lookuptable)
    print query1
    cur.execute(query1);
    conn.commit()

    query2 = 'update pre.{} set mtr = 5, yfc = NULL where traj_array in (select * from ('+str1+') ou where (select count(*) from ('+str1+') inr where inr.traj_array = ou.traj_array) > 1)'.format(lookuptable)
    print query2
    cur.execute(query2);
    conn.commit()



# removeNonMutuallyExclusive()
# NOTE: UPDATE TABLE TO REMOVE NOT MUTUALLY EXCLUSIIVE YFC AND YTC
# update pre.lookup_2008to2016_v3 set mtr = 5 where ytc IS NOT NULL AND yfc IS NOT NULL
# update pre.lookup_2008to2016_v3 set ytc = NULL, yfc = NULL where mtr = 5





# def updateYTC_version3():
#     #Note: this is a aux function that the reclassifyRaster() function references
#     cur = conn.cursor()

#     query = 'SELECT index,traj_array FROM pre.lookup_2008to2016_v3_t10'
#     #DDL: add column to hold arrays
#     cur.execute(query);

#     #create empty list
#     reclassifylist=[]

#     # fetch all rows from table
#     rows = cur.fetchall()

#     # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
#     for row in rows:
#         index = row[0]
#         traj_array = row[1]
#         if sum(traj_array) >= 7:
#             if [0,1,1] == traj_array[:3]:
#                 print sum(traj_array)
#                 print traj_array

#                 #DML: insert values into new array column
#                 cur.execute('UPDATE pre.{} SET ytc = {} where index = {};'.format('lookup_2008to2016_v3_t10', '2009', index));

#                 conn.commit()








####  these functions create the trajectory table  #############
# createTrajectories()
# addGDBTable2postgres()
# createRefinedTrajectory()


#######  these functions are to update the lookup tables  ######
labelByMTR()
# FindRedundantTrajectories()
# removeNonMutuallyExclusive()
# updateYTC_version3()
