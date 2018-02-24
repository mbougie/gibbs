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
import re



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


# importCSVtoPG()






# outCon = Con((path_mtr == yxc['ytc']) & (path_yxc >= 2008), path_yxc)
# outCon.save(path_mask)






def formatTable_cols2rows():
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = "SELECT * FROM counts_states.s17_ytc"
    
    df = pd.read_sql_query(query, engine)
    # print df
    tryit = {}
    for index, row in df.iterrows():
        
        temp = {}
        temp[2009]=row[1]
        temp[2010]=row[2]
        temp[2011]=row[3]
        temp[2012]=row[4]
        temp[2013]=row[5]
        temp[2014]=row[6]
        temp[2015]=row[7]
        temp[2016]=row[8]
   
        tryit.update({row[0]:temp})


    return tryit



def main_func():
    d=formatTable_cols2rows()

    full_list = []
    for key, value in d.iteritems():
        templist = []
        for year, conv in value.iteritems():
            templist = [key,year,conv]
            full_list.append(templist)

    print full_list
    df2 = pd.DataFrame(full_list, columns=['state', 'year', 'conv'])
    print df2

    df2.to_csv('C:\\Users\\Bougie\\Desktop\\temp\\test_r\\reformated.csv', sep=',')


def CreateUnionQuery(schema):
    cur = conn.cursor()

    tables = gen.getTablesInSchema(schema)
    unionlist=[]
    for table in tables:
        query="SELECT * from {}.{}".format(schema, table[0])
        unionlist.append(query)

    print unionlist
    unionstring=' UNION '.join(unionlist)
    query = 'CREATE TABLE test_union.test_it as '+unionstring
    print query
    # cur.execute(query)
    # conn.commit()







#####  create reclassified confidence  ######################################
def getFCvalues(fc, field, where):
    cursor = arcpy.da.SearchCursor(fc, field, where)
    for row in cursor:
        print int(row[0])
        return int(row[0])





def main_current():
    source = 'E:\\archive\\data\\CDL_confidence\\2016\\'
    gdb_out='E:\\archive\\data\\hold.gdb\\'
    # matches = []
    for root, dirnames, filenames in os.walk(source):
        for filename in fnmatch.filter(filenames, '*.img'):
            print root
            print filename
            path = root +'\\'+ filename

            ##get the state name from the image name
            result = re.search('cdl_30m_r_(.*)_2016_albers_confidence.img', filename)
            state = result.group(1)
            print state
            if state != 'dc':
                #####get the associated atlas_st for each state
                atlas_st = getFCvalues("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\states", "atlas_st", "st_abbrev = '{}'".format(state.upper()))
            
                    
                output = SetNull(path, atlas_st, "VALUE = 0")
                output.save(gdb_out+filename.replace('.img', '_atlas_st'))
  




def mosiacRasters():
    arcpy.env.workspace = 'E:\\archive\\data\\hold.gdb\\'

    rasterslist = arcpy.ListDatasets()

    reference_raster = Raster('cdl_30m_r_ar_2016_albers_confidence_atlas_st')

    ######mosiac tiles together into a new raster
    arcpy.MosaicToNewRaster_management(rasterslist, 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\conf.gdb', 'mosiac_conf', reference_raster.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\conf.gdb\\mosiac_conf', "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\conf.gdb\\mosiac_conf')












#####  call the function  ##################################
# main_func()
# CreateUnionQuery('test')
# main_current()
# mosiacRasters()

