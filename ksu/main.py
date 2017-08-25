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
# import general as gen 



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"

###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
# rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
# def defineGDBpath(arg_list):
#     gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
#     print 'gdb path: ', gdb_path 
#     return gdb_path

rootpath = 'D:/projects/CLU_fields_by_county/'


def main():
    #get the sub-directories names
    for subdir in os.listdir(rootpath):
        # if subdir == 'CO_shps':
        print subdir

        createGDB(subdir)

        for file in glob.glob(rootpath+subdir+'/*.shp'):
            print file

            addFeatureClass(subdir, file)








def createGDB(subdir):
    out_folder_path = "D:/projects/samples" 
    print subdir[:2]
    out_name = subdir[:2]+".gdb"

    # Execute CreateFileGDB
    arcpy.CreateFileGDB_management(out_folder_path, out_name)



def addFeatureClass(subdir, file):
    # Set environment settings
    env.workspace = "D:/projects/samples/"+subdir[:2]+".gdb"

    #  Set local variables
    inFeatures = file
    outFeatureClass = file[-9:-4]
    # outFeatureClass = 'D:/projects/temp/'+file[-9:-4]+'.shp'
    print 'outFeatureClass', outFeatureClass
    county = file[-7:-4]
    state = file[-9:-7]
    print 'state', state
    print 'county', county 

    # Use FeatureToPoint function to find a point inside each park
    arcpy.FeatureToPoint_management(inFeatures, outFeatureClass, "INSIDE")

    addFIPSField(outFeatureClass, state, county)



def addFIPSField(outFeatureClass, state, county):
    # add field to shapefile
    arcpy.AddField_management(in_table=outFeatureClass, field_name='fips', field_type="TEXT")

    # populate fips field
    cur = arcpy.UpdateCursor(outFeatureClass)

    fips = getStateFIPS(state) + county
    print type(fips)
    print fips
     
    for row in cur:
        row.setValue('fips', fips)
        cur.updateRow(row)


def getStateFIPS(state):
    print state
    state_abbrev = "'" + state.upper() + "'"
    print state
    cur = conn.cursor()
    query="SELECT atlas_st FROM spatial.states WHERE st_abbrev = " + state_abbrev
    cur.execute(query)
    rows = cur.fetchall()
    print rows
    for row in rows:
        print type(row[0])
        return row[0]




#####  call main function ##################
main()




