from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2

import general as g



conv_coef=0.000247105
# '''CONSTANTS'''
# 0.774922476



# class acres2(object):
#     print pixel_count
#     def __init__(self, pixel_count, resolution):
#         self.resolution= resolution


#     def conv(self, pixel_count, resolution):
#         if resolution == 56:
#             acres = pixel_count*0.774922476
#             print acres
#             return acres
#         elif resolution == 30:
#             return (pixel_count*0.222395)






























'''######## DEFINE THESE EACH TIME ##########'''
#NOTE: need to declare if want to process ytc or yfc
yxc = 'yfc'

#the associated mtr value qwith the yxc
yxc_mtr = {'ytc':'3', 'yfc':'4'}

#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")




try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.CheckOutExtension("Spatial")



###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 


##  datasets from core process ##########################################################
mmu_gdb=defineGDBpath(['core','mmu'])
mmu='traj_rfnd_n8h_mtr_8w_msk23_nbl'
mmu_Raster=Raster(mmu_gdb + mmu)



def getRasterCount(gdb_path, wc):
    arcpy.env.workspace = defineGDBpath(gdb_path)

    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',raster

        list_count=[]
        list_acres=[]


        
        #loop through each row and get the value for specified columns
        rows = arcpy.SearchCursor(raster)
        for row in rows:
        
            count = row.getValue('Count')
            # print count
            list_count.append(count)
            
            res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")

            #convert the result object into and integer
            res = res.getOutput(0)
            
            hi=acres(int(res))
            coef=hi.conv(int(res))
            # print 'coef', coef

            acreage = count * coef
            list_acres.append(acreage)



        cur = conn.cursor()
        query="INSERT INTO qaqc.counts VALUES (DEFAULT, '" + str(raster) + "' , " + str(sum(list_count)) + " , " + str(res) + " , " + str(sum(list_acres))+ ")"
        print query
        cur.execute(query)
        conn.commit()


def countDiff(child, parent,comment):
    
    sq1= '''(SELECT count_pixel-(SELECT 
              sum(count_pixel)
            FROM 
              qaqc.lookup_inheritance, 
              qaqc.counts
            WHERE 
              counts.dataset = lookup_inheritance.dataset and parent = '''+parent+'''
            group by
              lookup_inheritance.parent)
            FROM 
              qaqc.counts
            where dataset = '''+parent+')'


    sq2 = '''(SELECT count_acres-(SELECT 
              sum(count_acres)
            FROM 
              qaqc.lookup_inheritance, 
              qaqc.counts
            WHERE 
              counts.dataset = lookup_inheritance.dataset and parent = '''+parent+'''
            group by
              lookup_inheritance.parent)
            FROM 
              qaqc.counts
             where dataset = '''+parent+')'


    sq3 = '''1 - (SELECT(SELECT 
              sum(count_pixel)
            FROM 
              qaqc.lookup_inheritance, 
              qaqc.counts
            WHERE 
              counts.dataset = lookup_inheritance.dataset and parent = '''+parent+'''
            group by
              lookup_inheritance.parent)/count_pixel
            FROM 
              qaqc.counts
            where dataset = '''+parent+')'


    cur = conn.cursor()
    query = "INSERT INTO qaqc.counts_diff VALUES (DEFAULT, "+child+ "," +parent+ "," + sq1 + "," + sq2 + "," + sq3 + "," + comment +")"
    print query
    cur.execute(query)
    conn.commit()


def getPGTablesList():
    query = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'deliverables'
    AND table_name   LIKE 'gsConv_%_lcc_counties'"""
   
    
    templist = []
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print row[0]
        templist.append(row[0])
    return templist


def getPGColumnsList(table):
    query1 = """SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'deliverables'
    AND table_name = '""" + table + """' 
    AND column_name LIKE '%VALUE%' """
    print query1
    templist = []
    cur = conn.cursor()
    cur.execute(query1)
    rows = cur.fetchall()
    for row in rows:
        print row[0]
        templist.append(row[0])
    str1 = '+'.join('"{0}"'.format(w) for w in templist)
    # str1 = '" + "'.join(templist)
    print "str1", str1
    return str1



def getTableCount():
    tables = getPGTablesList()
    print tables

    for table in tables:
        print table
        columnlist=getPGColumnsList(table)
        query1='SELECT sum('+columnlist+') FROM deliverables."'+table+'"'
        query2="SELECT resolution FROM qaqc.counts_rasters as a JOIN qaqc.lookup_inheritance as b ON a.dataset = b.parent where b.dataset = '"+table+"'"
        
        count = g.fetchPG(query1)
        print count
        res = g.fetchPG(query2)
        print res

        query3="INSERT INTO qaqc.counts_tables VALUES ('" + table + "' , " + str(count) + " , " + str(conv_coef) + " , " + str(count*conv_coef)+ ")"
        # commitPG(query3)

    
new()

    
def addGDBTable2postgres(gdb_args,wc,pg_shema):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)

    for table in arcpy.ListTables(wc): 
        print 'table: ', table

        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(table)]
        
        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(table,fields)
        print arr
        
        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df
        
        # use pandas method to import table into psotgres
        df.to_sql(table, engine, schema=pg_shema)





##########  call addGDBTable2postgres  #########################################
# addGDBTable2postgres(['deliverables','deliverables_refined'],'gsConv_*_lcc_counties','deliverables')

######  call getRasterCount() function  ##############################
# getRasterCount(['deliverables','deliverables_refined'],'*')
# getRasterCount(['deliverables','xp_update_refined'],'*')
# getRasterCount(['ancillary','data_2008_2012'],'*class_before_crop*')


######  call getTableCount() function  ##############################
# getTableCount()


##########  call countDiff() function  #######################################
# countDiff("'gsConv_old'", "'class_before_crop'", "'subset to grassland and shrubland AND class0 is included in count'")
# countDiff("'gsConv_new'", "'bfc'", "'subset to grassland and shrubland'")
# countDiff("'gsConv_new_lcc'", "'gsConv_new'", "'lcc is null maybe'")
# countDiff("'gsConv_old_lcc'","'gsConv_old'", "'lcc is null maybe'")










