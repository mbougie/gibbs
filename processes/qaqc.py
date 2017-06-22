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



# conv_coef=0.000247105
# '''CONSTANTS'''
# 0.774922476

coef={'acres':1,'msq':0.000247105,'30m':0.222395,'56m':0.774922476}

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
            print count
            print type(count)
            list_count.append(count)
            
            res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")

            #convert the result object into and integer
            res = res.getOutput(0)
            print res
            print type(res)
            acreage = g.getAcres(int(count), int(res))
            list_acres.append(acreage)
            
           



        cur = conn.cursor()
        query="INSERT INTO qaqc.counts_rasters VALUES ('" + str(raster) + "' , " + str(sum(list_count)) + " , " + str(res) + " , " + str(sum(list_acres))+ ")"
        print query
        cur.execute(query)
        conn.commit()






def countDiff(table):
    query=  """ SELECT 
                  b.child,
                  a.acres, 
                  b.parent,
                  c.acres,
                  c.acres - a.acres,
                  (1 - (a.acres / c.acres)) * 100
                FROM 
                  qaqc."""+ table + """ as a, 
                  qaqc.lookup_inheritance as b, 
                  qaqc.counts_rasters as c
                WHERE 
                  b.child = a.dataset AND
                  b.parent = c.dataset AND 
                  b.process = TRUE"""
    print query
    rows=g.fetchPG(query)
    for row in rows:
        print row
        cur = conn.cursor()
        query = "UPDATE qaqc.counts_diff2 SET (child, acres_child, parent, acres_parent, diff_acres, diff_perc) = "+str(row)+" WHERE child = '"+ str(row[0]) + "'"
        
        print query
        cur.execute(query)
        conn.commit()




######################   NEW     ####################################################################################

def getTableCount(dataset):

    schema = "'deliverables'"
    # dataset = "'gsconv_%_lcc_counties'"
    tables = g.getPGTablesList(schema,dataset)
    print tables

    for table in tables:
        table_w_qoutes= "'"+table+"'"
        # wc="'%value%'"
        columnlist=g.getPGColumnsList(schema, table_w_qoutes)

        query1='SELECT sum('+columnlist+') FROM deliverables.'+table
        print query1
        
        query2="SELECT units FROM qaqc.lookup_inheritance where child = "+table_w_qoutes
        print query2
        count = g.fetchPG(query1)
        print count
        unit = g.fetchPG(query2)
        print unit
        coefficient=coef.get(unit[0][0])
        print coefficient
        

        query3="INSERT INTO qaqc.counts_tables VALUES (" + table_w_qoutes + " , " + str(count[0][0]*coefficient)+ ")"
        g.commitPG(query3)






def getDerivedTableCounts(parent):
    out_table="'"+parent+'_lcc'+"'"
    query1= """ SELECT 
                    a.resolution,
                    sum(a.acres)
                FROM 
                    qaqc.counts_rasters as a,
                    qaqc.lookup_inheritance as b

                WHERE 
                    a.dataset = b.dataset AND
                    b.parent = '""" + parent + """'
                GROUP BY a.resolution
            """
    print query1
    result = g.fetchPG(query1)

    query3="INSERT INTO qaqc.counts_tables VALUES (" + out_table +" , " + str(result[0][1]) + ")"
    g.commitPG(query3)

    


    






##########  call addGDBTable2postgres  #########################################
# g.addGDBTable2postgres(['deliverables','deliverables_refined'],'mtr_counties','deliverables')

######  call getRasterCount() function  ##############################
# getRasterCount(['deliverables','deliverables_refined'],'*')
# getRasterCount(['deliverables','xp_update_refined'],'*')
# getRasterCount(['ancillary','cdl'],'*cdl_2012*')



######  call getTableCounts() function  ##############################
# getTableCount("'mtr_%_counties'","'%value%'")
# getTableCount("'gsconv_%_lcc_counties'")




######  call getDerivedTableCounts() function  ##############################
# getDerivedTableCounts("gsconv_new")


##########  call countDiff() function  #######################################
# countDiff("'gsConv_old'", "'class_before_crop'", "'subset to grassland and shrubland AND class0 is included in count'")
# countDiff("'gsConv_new'", "'bfc'", "'subset to grassland and shrubland'")
# countDiff("'gsConv_new_lcc'", "'gsConv_new'", "'lcc is null maybe'")
# countDiff("'gsConv_old_lcc'","'gsConv_old'", "'lcc is null maybe'")



countDiff('counts_rasters')



# g.addRasterAttributeTable(['deliverables','xp_update_refined'],'mtr')









