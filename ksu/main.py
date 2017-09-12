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
import fnmatch

# import general as gen 

case=['Bougie','Gibbs']

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
# rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

# ### establish gdb path  ####
# def defineGDBpath(arg_list):
#     gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
#     print 'gdb path: ', gdb_path 
#     return gdb_path




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

rootpath = 'D:/projects/ksu/control/CLU_fields_by_county/'



def mosiacCLU():
    ##get the sub-directories names
    for subdir in os.listdir(rootpath):
        # if subdir == 'AR_shps':
        print subdir
        filelist = []
        for file in glob.glob(rootpath+subdir+'/*.shp'):
            print file
            filelist.append(file)
        print filelist












def main():
    ###get the sub-directories names
    # for subdir in os.listdir(rootpath):
    #     # if subdir == 'AR_shps':
    #     print subdir

    #     createGDB(subdir)

    #     for file in glob.glob(rootpath+subdir+'/*.shp'):
    #         print file

    #         addFeatureClass(subdir, file)


    ###merge all samples into one featureclass
    # mergeSamples()

    ###mosaic datasets by year 
    # years = ['2001','2002','2003','2004','2005','2006','2007']
    years = ['2005','2006','2007']
    res_list = ['30','56']
    for year in years:
        for res in res_list:
            mosaicCDL(year,res)
    








def mergeSamples():
    arcpy.env.workspace = "D:/projects/ksu/samples"

    # List all file geodatabases in the current workspace
    workspaces = arcpy.ListWorkspaces("*", "FileGDB")
    
    completelist = []
    # return workspaces
    for workspace in workspaces:
        print workspace
        # if workspace == 'D:/projects/ksu/samples\AR.gdb':
        
        arcpy.env.workspace = workspace

        featureclasses = arcpy.ListFeatureClasses()

        print featureclasses

        for fc in featureclasses:
            print fc

            print workspace + '/' + fc
            completelist.append(workspace + '/' + fc)

    print completelist
    arcpy.Merge_management(completelist, "D:/projects/ksu/attributes.gdb/tryit2")



def createGDB(subdir):
    out_folder_path = "D:/projects/ksu/samples" 
    print subdir[:2]
    out_name = subdir[:2]+".gdb"

    # Execute CreateFileGDB
    arcpy.CreateFileGDB_management(out_folder_path, out_name)



def addFeatureClass(subdir, file):
    # Set environment settings
    env.workspace = "D:/projects/ksu/samples/"+subdir[:2]+".gdb"

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








def mosaicCDL(year,res):
    tif_array = getFilesRecursively(year,res)
    print tif_array
    if not tif_array:
        print("List is empty")
   
    else:
        filename = 'cdl'+res+'_'+year
        createEmptyRaster(filename)


        if len(tif_array) == 1:
            stringit = tif_array[0]
            print stringit
            arcpy.Mosaic_management(inputs=stringit, target="D:/projects/ksu/cdl.gdb/"+filename, background_value=0, nodata_value=0)

            # projectRaster("D:/projects/ksu/cdl.gdb/"+filename)
            


        elif len(tif_array) > 1:
            stringit = ';'.join(tif_array)
            print stringit
            arcpy.Mosaic_management(inputs=stringit, target="D:/projects/ksu/cdl.gdb/"+filename, background_value=0, nodata_value=0)
            
            # projectRaster("D:/projects/ksu/cdl.gdb/"+filename)







def getFilesRecursively(year, res):
    rootpath = 'D:/projects/ksu/control/cdl'
    wc = '*'+res+'*'+year+'*.tif'
    print 'wc:', wc
    matches = []
    for root, dirnames, filenames in os.walk(rootpath):
        for filename in fnmatch.filter(filenames, wc):
            print filename
            matches.append(os.path.join(root, filename)) 
    # print matches
    return matches



def createEmptyRaster(filename):
    #create an empty raster to hold mosaic dataset
    arcpy.CreateRasterDataset_management(out_path="D:/projects/ksu/attributes.gdb", out_name=filename, pixel_type="8_BIT_UNSIGNED", number_of_bands=1)


# def projectRaster(filename):
#     dataset = "C:/Users/Bougie/Desktop/Gibbs/data/usxp/ancillary/cdl.gdb/cdl30_2013"
#     spatial_ref = arcpy.Describe(dataset).spatialReference
#     print spatial_ref.name

#     # run project tool
#     arcpy.ProjectRaster_management(filename, filename+'_acea2', "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "NEAREST", "30 30", "", "", "PROJCS['LUnits_meters',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meter',1.0]]")







#####  call main function ##################
# main()

mosiacCLU()

