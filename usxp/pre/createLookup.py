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





def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template




data = getJSONfile()
print data







def addTrajArrayField(fields):
    #this is a sub function for addGDBTable2postgres()
    cur = conn.cursor()
    
    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE pre.{} ADD COLUMN traj_array integer[];'.format(data['pre']['traj']['filename']));
    
    #DML: insert values into new array column
    cur.execute('UPDATE pre.{} SET traj_array = ARRAY[{}];'.format(data['pre']['traj']['filename'], columnList));
    
    conn.commit()
    print "Records created successfully";
    conn.close()







def labelTrajectories():
    cur = conn.cursor()
    table = 'pre.traj_cdl'+pre.res+'_b_'+pre.datarange
    lookuptable = 'pre.traj_'''+pre.datarange+'_lookup'

    for year in pre.conversionyears:
        pre_context = 'cdl'+pre.res+'_b_'+str(year - 2)
        before_year ='cdl'+pre.res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+pre.res+'_b_'+str(year)
        post_context = 'cdl'+pre.res+'_b_'+str(year + 1)
        query_ytc = 'update '+lookuptable+' set mtr = 3, ytc = '+str(year)+' where traj_array in (SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 0 AND '+before_year+'= 0 AND '+year_cdl+' = 1 AND '+post_context+' = 1 )'
        print query_ytc
        cur.execute(query_ytc)
        conn.commit()


        query_yfc = 'update '+lookuptable+' set mtr = 4, yfc = '+str(year)+' where traj_array in (SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 1 AND '+before_year+'= 1 AND '+year_cdl+' = 0 AND '+post_context+' = 0 )'
        print query_yfc
        cur.execute(query_yfc)
        conn.commit()






def FindRedundantTrajectories():
    # what is the purpose of this function??
    cur = conn.cursor()
    table = 'pre.traj_cdl{0}_b_{1}'.format(pre.res, pre.datarange)
    lookuptable = 'pre.traj_{}_lookup'.format(pre.datarange)

    query_list = []
    for year in pre.conversionyears:
        pre_context = 'cdl'+pre.res+'_b_'+str(year - 2)
        before_year ='cdl'+pre.res+'_b_'+str(year - 1)
        year_cdl = 'cdl'+pre.res+'_b_'+str(year)
        post_context = 'cdl'+pre.res+'_b_'+str(year + 1)
        query_ytc = 'SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 0 AND '+before_year+'= 0 AND '+year_cdl+' = 1 AND '+post_context+' = 1'
        print query_ytc
        query_list.append(query_ytc)

        query_yfc = 'SELECT traj_array FROM '+table+' a INNER JOIN '+lookuptable+' b using(traj_array) Where '+pre_context+' = 1 AND '+before_year+'= 1 AND '+year_cdl+' = 0 AND '+post_context+' = 0'
        print query_yfc
        query_list.append(query_yfc)



    print query_list
    str1 = ' UNION ALL '.join(query_list)
    print str1


    query1 = 'update pre.traj_2008to2016_lookup set mtr = 5, ytc = NULL where traj_array in (select * from ('+str1+') ou where (select count(*) from ('+str1+') inr where inr.traj_array = ou.traj_array) > 1)'
    print query1
    cur.execute(query1);
    conn.commit()

    query2 = 'update pre.traj_2008to2016_lookup set mtr = 5, yfc = NULL where traj_array in (select * from ('+str1+') ou where (select count(*) from ('+str1+') inr where inr.traj_array = ou.traj_array) > 1)'
    print query2
    cur.execute(query2);
    conn.commit()






def updateYTC_version3():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()

    query = 'SELECT index,traj_array FROM pre.lookup_2008to2016_v3_t10'
    #DDL: add column to hold arrays
    cur.execute(query);

    #create empty list
    reclassifylist=[]

    # fetch all rows from table
    rows = cur.fetchall()

    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        index = row[0]
        traj_array = row[1]
        if sum(traj_array) >= 7:
            if [0,1,1] == traj_array[:3]:
                print sum(traj_array)
                print traj_array

                #DML: insert values into new array column
                cur.execute('UPDATE pre.{} SET ytc = {} where index = {};'.format('lookup_2008to2016_v3_t10', '2009', index));

                conn.commit()








####  these functions create the trajectory table  #############
# createTrajectories()
# addGDBTable2postgres()
# createRefinedTrajectory()


#######  these functions are to update the lookup tables  ######
# labelTrajectories()
# FindRedundantTrajectories()
updateYTC_version3()
