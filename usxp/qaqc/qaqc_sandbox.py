from sqlalchemy import create_engine
import numpy as np, sys, os
import fnmatch
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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen




'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'D:\\projects\\usxp\\'

### establish gdb path  ####
def defineGDBpath(args_list):
    gdb_path = '{}{}/{}.gdb/'.format(rootpath,args_list[0],args_list[1])
    # print 'gdb path: ', gdb_path 
    return gdb_path

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"








def importCSVtoPG():

    df = pd.read_excel('C:\\Users\\Bougie\\Desktop\\noncropland_cropland_county.xls')
    df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces

    from sqlalchemy import create_engine
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    df.to_sql("fsa_2012", engine, schema='qaqc')


importCSVtoPG()