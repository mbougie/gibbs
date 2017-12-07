from sqlalchemy import create_engine
import numpy as np, sys, os
import gdal
import subprocess
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
import rasterstats

# import general as gen 

case=['Bougie','Gibbs']

try:
    conn = psycopg2.connect("dbname='ksu' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
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

# rootpath = 'D:/projects/ksu/control/clu/CLU_fields_by_county/'



rootpath = 'D:/projects/ksu/v2/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


def main():
    ###get the sub-directories names
    # for subdir in os.listdir(rootpath):
    #     if subdir == 'AR_shps':
    #         print subdir

            # createGDB(subdir)
            # importShpToFC(subdir)
            # bufferit(subdir)
            # unionit(subdir)


            # for file in glob.glob(rootpath+subdir+'/*.shp'):
            #     print file

                # addFeatureClass(subdir, file)




    ###merge all samples into one featureclass
    # mergeSamples()

    ###mosaic datasets by year 



    years = range(2008,2017)
    print years
    res_list = ['30','56']
    for year in years:
        for res in res_list:
            print res
            # mosaicCDL(str(year),res)
            # reprojectRasters(str(year),res)

    





def createGDB(subdir):
    out_folder_path = "D:/projects/ksu/v2/clu_dbs" 
    print subdir[:2]
    out_name = subdir[:2]+".gdb"

    # Execute CreateFileGDB
    arcpy.CreateFileGDB_management(out_folder_path, out_name)


def importShpToFC(subdir):
    env.workspace = 'D:/projects/ksu/control/clu/CLU_fields_by_county/'+subdir
    fcs = arcpy.ListFeatureClasses("*")  
    for fc in fcs:  
        print fc
        arcpy.FeatureClassToGeodatabase_conversion(fc, "D:/projects/ksu/v2/clu_dbs/"+subdir[:2]+".gdb") 


def bufferit(subdir):
    env.workspace = "D:/projects/ksu/v2/clu_dbs/"+subdir[:2]+".gdb"
    fcs = arcpy.ListFeatureClasses("*")  
    
    for fc in fcs: 
        print fc
        arcpy.Buffer_analysis(fc, fc+'_buff_1cm', "1 Centimeters", "FULL", "ROUND", "NONE", "", "PLANAR")



def unionit(subdir):
    # env.workspace = "D:/projects/ksu/v2/clu_dbs/"+subdir[:2]+".gdb"
    # fcs = arcpy.ListFeatureClasses("*buff_1cm")  
    
    # for fc in fcs:
    #     print fc

        
        #set the extent of the clu dataset
        # desc = getExtent(fc)
        # arcpy.env.extent = arcpy.Extent(desc.XMin, desc.YMin, desc.XMax, desc.YMax)
        

    templist = ['D:\\projects\\ksu\\v2\\tiles_test.gdb\\tile_clu', 'D:\\projects\\ksu\\v2\\tiles_test.gdb\\tile_yans'] 
    # templist.append(fc)
    print templist
    
    arcpy.Union_analysis(templist, 'D:\\projects\\ksu\\v2\\tiles_test.gdb\\tile_union_python', "ALL", "45 Meters", "GAPS")
    # arcpy.Union_analysis("D:\\projects\\ksu\\v2\\clu_dbs\\AR.gdb\\clu_public_a_ar001_buff_1cm #;D:\\projects\\ksu\\v2\\yan_roy.gdb\\yan_roy #", clu_public_a_ar001_buff_1cm_, "ALL", "45 Meters", "GAPS")
    # arcpy.ClearEnvironment("extent")



def getExtent(fc):
    for row in arcpy.da.SearchCursor(fc, ["SHAPE@", "BUFF_DIST"]):
        extent = row[0].extent
        return extent
























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
























def addFeatureClass(subdir, file):
    # Set environment settings
    env.workspace = "D:/projects/ksu/v2/clu_dbs/"+subdir[:2]+".gdb"

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
    outpath = "D:/projects/ksu/v2/rasters/"
    filename = 'cdl'+res+'_'+year
    filepath = outpath+filename
    

    if not tif_array:
        print("List is empty")
   
    else:
        if len(tif_array) == 1:
            createEmptyRaster(outpath,filename)
            stringit = tif_array[0]
            print stringit
            arcpy.Mosaic_management(inputs=stringit, target=filepath+'.img', background_value=0, nodata_value=0)


        elif len(tif_array) > 1:
            createEmptyRaster(outpath,filename)
            stringit = ';'.join(tif_array)
            print stringit
            arcpy.Mosaic_management(inputs=stringit, target=filepath+'.img', background_value=0, nodata_value=0)



def reprojectRasters(year,res):
    outpath = "D:/projects/ksu/v2/rasters/"
    filename = 'cdl'+res+'_'+year
    filepath = outpath+filename
    command = 'gdalwarp -overwrite -t_srs EPSG:5070 -of HFA '+filepath+'.img'+' '+filepath+'_5070.img'
    print command
    os.system(command)



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



def createEmptyRaster(outpath, filename):
    #create an empty raster to hold mosaic dataset
    arcpy.CreateRasterDataset_management(out_path=outpath, out_name=filename+'.img', pixel_type="8_BIT_UNSIGNED", number_of_bands=1)


# def projectRaster(filename):
#     dataset = "C:/Users/Bougie/Desktop/Gibbs/data/usxp/ancillary/cdl.gdb/cdl30_2013"
#     spatial_ref = arcpy.Describe(dataset).spatialReference
#     print spatial_ref.name

#     # run project tool
#     arcpy.ProjectRaster_management(filename, filename+'_acea2', "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "NEAREST", "30 30", "", "", "PROJCS['LUnits_meters',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meter',1.0]]")








def clipFCtoFishnet():
    # self.out_fishnet = defineGDBpath(['ancillary', 'shapefiles']) + 'fishnet_subset'
    arcpy.env.workspace = defineGDBpath(['ancillary', 'shapefiles'])
    # Use the ListFeatureClasses function to return a list of shapefiles.
    fc = 'fishnet_subset'

    cursor = arcpy.da.SearchCursor(fc, ['OBJECTID'])
    for row in cursor:
        print(row[0])
        
        layer = "layer_" + str(row[0])
        where_clause = "OBJECTID = " + str(row[0])

 


        # Set local variables
        in_features = defineGDBpath(['main', 'merged_clu']) + 'clu2008county_1cm_buffer'
        clip_features = arcpy.MakeFeatureLayer_management(fc,layer, where_clause)
        out_feature_class = defineGDBpath(['main', 'yo']) + "stco_"+str(row[0])
        xy_tolerance = ""

        # Execute Clip
        arcpy.Clip_analysis(in_features, clip_features, out_feature_class, xy_tolerance)





def pgTableToFC():
    # sf = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/qaqc/sf/'+state.lower()+'.shp'
    # sf_acea = sf.replace(".shp", "_acea")

    command = 'ogr2ogr -f "PGDUMP" PG:"host=144.92.235.105 port=5432 dbname=ksu user=mbougie password=Mend0ta!" D:/projects/ksu/clu.gdb -nlt PROMOTE_TO_MULTI -nln yo.try_it clu_samples -overwrite -progress --config PG_USE_COPY YES'
    print command
    os.system(command)





def clipFCtoCounty():
    
    arcpy.env.workspace = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/ancillary/shapefiles.gdb/'

    # Use the ListFeatureClasses function to return a list of shapefiles.
    fc = 'states'

    cursor = arcpy.da.SearchCursor(fc, ['st_abbrev'])
    for row in cursor:
        print(row[0])
        
        layer = "layer_" + row[0]
        where_clause = "st_abbrev = '" + row[0] + "'"
        clip_features = arcpy.MakeFeatureLayer_management(fc,layer, where_clause)
 


        # # Set local variables
        in_features = defineGDBpath(['main','samples'])+'merged'
        out_feature_class = rootpath+"main/sf/sample_"+row[0]
        xy_tolerance = ""

        # # Execute Clip
        arcpy.Clip_analysis(in_features, clip_features, out_feature_class, xy_tolerance)



















#####  call main function ##################
# main()
# clipFCtoCounty()



# def mergeYansRoyData():
    #####  
    #gdal_merge.py -init 255 -o  D:\projects\ksu\out.tif D:\projects\ksu\control\ReleaseData\h07v04\WELD_h07v04_2010_field_segments D:\projects\ksu\control\ReleaseData\h10v14\WELD_h10v14_2010_field_segments




    # def getFilesRecursively_t2():
    #     rootpath = 'D:/projects/ksu/control/ReleaseData'
    #     wc = '*_segments'
    #     print 'wc:', wc
    #     matches = []
    #     for root, dirnames, filenames in os.walk(rootpath):
    #         # print filenames
    #         for filename in fnmatch.filter(filenames, wc):
    #             print filename
    #             matches.append(os.path.join(root, filename)) 

    #     # print matches
    #     return matches

    # filelist = getFilesRecursively_t2()
    # print filelist
    # filelist = ['D:/projects/ksu/control/ReleaseData\\h01v08\\WELD_h01v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v03\\WELD_h02v03_2010_field_segments']


    # # str1 = ','.join(filelist)
    # # print str1



    # merge_command = ["python", "C:\Python27\ArcGISx6410.4\Lib\site-packages\osgeo\scripts\gdal_merge.py", "-o", "D:\projects\ksu\out_t2.tif", 'D:/projects/ksu/control/ReleaseData\\h01v08\\WELD_h01v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v03\\WELD_h02v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v04\\WELD_h02v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v06\\WELD_h02v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v07\\WELD_h02v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v08\\WELD_h02v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v09\\WELD_h02v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v10\\WELD_h02v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h02v11\\WELD_h02v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v03\\WELD_h03v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v04\\WELD_h03v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v05\\WELD_h03v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v06\\WELD_h03v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v07\\WELD_h03v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v08\\WELD_h03v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v09\\WELD_h03v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v10\\WELD_h03v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v11\\WELD_h03v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h03v12\\WELD_h03v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v01\\WELD_h04v01_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v02\\WELD_h04v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v03\\WELD_h04v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v04\\WELD_h04v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v05\\WELD_h04v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v06\\WELD_h04v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v07\\WELD_h04v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v08\\WELD_h04v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v09\\WELD_h04v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v11\\WELD_h04v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v12\\WELD_h04v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h04v13\\WELD_h04v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v01\\WELD_h05v01_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v02\\WELD_h05v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v03\\WELD_h05v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v04\\WELD_h05v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v05\\WELD_h05v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v06\\WELD_h05v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v07\\WELD_h05v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v08\\WELD_h05v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v12\\WELD_h05v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h05v13\\WELD_h05v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v01\\WELD_h06v01_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v02\\WELD_h06v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v03\\WELD_h06v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v04\\WELD_h06v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v05\\WELD_h06v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v06\\WELD_h06v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v07\\WELD_h06v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v08\\WELD_h06v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v09\\WELD_h06v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v10\\WELD_h06v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h06v13\\WELD_h06v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v02\\WELD_h07v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v03\\WELD_h07v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v04\\WELD_h07v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v05\\WELD_h07v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v06\\WELD_h07v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v07\\WELD_h07v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v08\\WELD_h07v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v09\\WELD_h07v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v10\\WELD_h07v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v13\\WELD_h07v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h07v14\\WELD_h07v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v02\\WELD_h08v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v03\\WELD_h08v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v04\\WELD_h08v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v05\\WELD_h08v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v06\\WELD_h08v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v07\\WELD_h08v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v08\\WELD_h08v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v09\\WELD_h08v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v10\\WELD_h08v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v14\\WELD_h08v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h08v15\\WELD_h08v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v02\\WELD_h09v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v03\\WELD_h09v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v04\\WELD_h09v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v05\\WELD_h09v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v06\\WELD_h09v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v07\\WELD_h09v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v08\\WELD_h09v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v09\\WELD_h09v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v10\\WELD_h09v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v11\\WELD_h09v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v14\\WELD_h09v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h09v15\\WELD_h09v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v02\\WELD_h10v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v03\\WELD_h10v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v04\\WELD_h10v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v05\\WELD_h10v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v06\\WELD_h10v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v08\\WELD_h10v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v09\\WELD_h10v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v10\\WELD_h10v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v11\\WELD_h10v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v12\\WELD_h10v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v13\\WELD_h10v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v14\\WELD_h10v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h10v15\\WELD_h10v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v02\\WELD_h11v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v03\\WELD_h11v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v04\\WELD_h11v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v05\\WELD_h11v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v06\\WELD_h11v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v07\\WELD_h11v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v08\\WELD_h11v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v09\\WELD_h11v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v10\\WELD_h11v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v11\\WELD_h11v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v12\\WELD_h11v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v13\\WELD_h11v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v14\\WELD_h11v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h11v15\\WELD_h11v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v02\\WELD_h12v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v03\\WELD_h12v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v04\\WELD_h12v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v05\\WELD_h12v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v06\\WELD_h12v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v07\\WELD_h12v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v08\\WELD_h12v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v09\\WELD_h12v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v10\\WELD_h12v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v11\\WELD_h12v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v12\\WELD_h12v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v13\\WELD_h12v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v14\\WELD_h12v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v15\\WELD_h12v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h12v16\\WELD_h12v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v02\\WELD_h13v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v03\\WELD_h13v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v04\\WELD_h13v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v05\\WELD_h13v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v06\\WELD_h13v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v07\\WELD_h13v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v08\\WELD_h13v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v09\\WELD_h13v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v10\\WELD_h13v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v11\\WELD_h13v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v12\\WELD_h13v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v13\\WELD_h13v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v14\\WELD_h13v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h13v15\\WELD_h13v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v02\\WELD_h14v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v03\\WELD_h14v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v04\\WELD_h14v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v05\\WELD_h14v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v06\\WELD_h14v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v07\\WELD_h14v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v08\\WELD_h14v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v09\\WELD_h14v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v10\\WELD_h14v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v11\\WELD_h14v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v12\\WELD_h14v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v13\\WELD_h14v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v14\\WELD_h14v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v15\\WELD_h14v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v16\\WELD_h14v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v17\\WELD_h14v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h14v18\\WELD_h14v18_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v02\\WELD_h15v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v03\\WELD_h15v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v04\\WELD_h15v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v05\\WELD_h15v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v06\\WELD_h15v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v07\\WELD_h15v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v08\\WELD_h15v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v09\\WELD_h15v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v10\\WELD_h15v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v11\\WELD_h15v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v12\\WELD_h15v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v13\\WELD_h15v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v14\\WELD_h15v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v15\\WELD_h15v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v16\\WELD_h15v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v17\\WELD_h15v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v18\\WELD_h15v18_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h15v19\\WELD_h15v19_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v02\\WELD_h16v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v03\\WELD_h16v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v04\\WELD_h16v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v05\\WELD_h16v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v06\\WELD_h16v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v07\\WELD_h16v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v08\\WELD_h16v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v09\\WELD_h16v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v10\\WELD_h16v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v11\\WELD_h16v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v12\\WELD_h16v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v13\\WELD_h16v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v14\\WELD_h16v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v15\\WELD_h16v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v16\\WELD_h16v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v17\\WELD_h16v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v18\\WELD_h16v18_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h16v19\\WELD_h16v19_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v02\\WELD_h17v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v03\\WELD_h17v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v04\\WELD_h17v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v05\\WELD_h17v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v06\\WELD_h17v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v07\\WELD_h17v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v08\\WELD_h17v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v09\\WELD_h17v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v10\\WELD_h17v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v11\\WELD_h17v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v12\\WELD_h17v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v13\\WELD_h17v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v14\\WELD_h17v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v15\\WELD_h17v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v16\\WELD_h17v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h17v17\\WELD_h17v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v04\\WELD_h18v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v05\\WELD_h18v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v06\\WELD_h18v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v07\\WELD_h18v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v08\\WELD_h18v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v09\\WELD_h18v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v10\\WELD_h18v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v11\\WELD_h18v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v12\\WELD_h18v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v13\\WELD_h18v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v14\\WELD_h18v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v15\\WELD_h18v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v16\\WELD_h18v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h18v17\\WELD_h18v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v04\\WELD_h19v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v05\\WELD_h19v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v06\\WELD_h19v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v07\\WELD_h19v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v08\\WELD_h19v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v09\\WELD_h19v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v10\\WELD_h19v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v12\\WELD_h19v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v13\\WELD_h19v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v14\\WELD_h19v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v15\\WELD_h19v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v16\\WELD_h19v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h19v17\\WELD_h19v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v05\\WELD_h20v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v06\\WELD_h20v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v07\\WELD_h20v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v08\\WELD_h20v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v09\\WELD_h20v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v10\\WELD_h20v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v11\\WELD_h20v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v12\\WELD_h20v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v13\\WELD_h20v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v14\\WELD_h20v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v15\\WELD_h20v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v16\\WELD_h20v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h20v17\\WELD_h20v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v04\\WELD_h21v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v05\\WELD_h21v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v06\\WELD_h21v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v07\\WELD_h21v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v08\\WELD_h21v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v09\\WELD_h21v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v10\\WELD_h21v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v11\\WELD_h21v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v12\\WELD_h21v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v13\\WELD_h21v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v14\\WELD_h21v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v15\\WELD_h21v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h21v16\\WELD_h21v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v04\\WELD_h22v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v05\\WELD_h22v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v06\\WELD_h22v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v07\\WELD_h22v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v08\\WELD_h22v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v09\\WELD_h22v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v10\\WELD_h22v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v11\\WELD_h22v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v12\\WELD_h22v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v13\\WELD_h22v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v14\\WELD_h22v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v15\\WELD_h22v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h22v16\\WELD_h22v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v04\\WELD_h23v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v05\\WELD_h23v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v06\\WELD_h23v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v07\\WELD_h23v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v08\\WELD_h23v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v09\\WELD_h23v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v10\\WELD_h23v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v11\\WELD_h23v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v12\\WELD_h23v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v13\\WELD_h23v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v14\\WELD_h23v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v15\\WELD_h23v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h23v16\\WELD_h23v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v06\\WELD_h24v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v07\\WELD_h24v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v08\\WELD_h24v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v09\\WELD_h24v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v10\\WELD_h24v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v11\\WELD_h24v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v12\\WELD_h24v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v13\\WELD_h24v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v14\\WELD_h24v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v15\\WELD_h24v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h24v16\\WELD_h24v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v07\\WELD_h25v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v08\\WELD_h25v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v09\\WELD_h25v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v10\\WELD_h25v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v11\\WELD_h25v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v12\\WELD_h25v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v13\\WELD_h25v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v14\\WELD_h25v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v15\\WELD_h25v15_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h25v16\\WELD_h25v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v06\\WELD_h26v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v07\\WELD_h26v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v08\\WELD_h26v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v09\\WELD_h26v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v10\\WELD_h26v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v11\\WELD_h26v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v12\\WELD_h26v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v13\\WELD_h26v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v14\\WELD_h26v14_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v16\\WELD_h26v16_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v17\\WELD_h26v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v18\\WELD_h26v18_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h26v19\\WELD_h26v19_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v04\\WELD_h27v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v05\\WELD_h27v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v06\\WELD_h27v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v07\\WELD_h27v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v08\\WELD_h27v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v09\\WELD_h27v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v10\\WELD_h27v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v11\\WELD_h27v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v12\\WELD_h27v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v13\\WELD_h27v13_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v17\\WELD_h27v17_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v18\\WELD_h27v18_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h27v19\\WELD_h27v19_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v04\\WELD_h28v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v05\\WELD_h28v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v06\\WELD_h28v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v07\\WELD_h28v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v08\\WELD_h28v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v09\\WELD_h28v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v10\\WELD_h28v10_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v11\\WELD_h28v11_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h28v12\\WELD_h28v12_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h29v04\\WELD_h29v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h29v05\\WELD_h29v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h29v06\\WELD_h29v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h29v07\\WELD_h29v07_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h29v08\\WELD_h29v08_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h29v09\\WELD_h29v09_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h30v02\\WELD_h30v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h30v03\\WELD_h30v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h30v04\\WELD_h30v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h30v05\\WELD_h30v05_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h30v06\\WELD_h30v06_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h31v02\\WELD_h31v02_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h31v03\\WELD_h31v03_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h31v04\\WELD_h31v04_2010_field_segments', 'D:/projects/ksu/control/ReleaseData\\h32v03\\WELD_h32v03_2010_field_segments'
    # ]
    # # subprocess.call(merge_command)








def addGDBTable2postgres():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu')
    
    # path to the table you want to import into postgres
    input = defineGDBpath(['v2','temp'])+'ark_corrected'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(pre.traj_dataset, engine, schema='clu')
    
    #add trajectory field to table
    addTrajArrayField(fields)



def formatTables():
    arcpy.env.workspace = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/ancillary/shapefiles.gdb/'

    # Use the ListFeatureClasses function to return a list of shapefiles.
    fc = 'states'

    cursor = arcpy.da.SearchCursor(fc, ['st_abbrev'])
    listit = []
    # donelist = [u'AL', u'AZ', u'AR', u'CA', u'CO', u'CT', u'DE', u'FL', u'GA', u'ID', u'IL', u'IN', u'IA', u'KS', u'KY', u'LA', u'ME', u'MD', u'MA', u'MI', u'MN', u'MS', u'MO', u'MT', u'NE', u'NV', u'NH', u'NJ', u'NM', u'NY', u'NC', u'ND', u'OH', u'OK', u'OR', u'PA', u'RI', u'SC', u'SD', u'TN', u'TX', u'UT', u'VT']
    donelist = ['WY']
    print cursor
    for row in cursor:
        # if (row[0] in donelist):
        print(row[0])
        # listit.append(row[0])
        # print listit

        layer = "layer_" + row[0]
        where_clause = "st_abbrev = '" + row[0] + "'"
        
        # addGDBTable2postgres(row[0])
        # createTable(row[0])
        exportPGtoCSV(row[0])


def addGDBTable2postgres(state):
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu')

    arcpy.env.workspace =  defineGDBpath(['main','samples'])
    
    wc = 'samples_merged_4152'
    print wc
# arcpy.ListDatasets(feature_type='feature')
    for fc in arcpy.ListFeatureClasses(wc): 

    # for table in arcpy.ListTables(wc): 
        print 'fc: ', fc
        
        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(fc)]
        # print fields
        print fields

        fields.remove('Shape')

        # fields = ['OBJECTID', 'POINT_X', 'POINT_Y', 'area_m2_1', 'atlas_st_1', 'st_abbrev_1', 'atlas_name_1', 'HUC8', 'MLRA_ID', 'MLRA_NAME', 'mukey_stgo', 'atlas_stco', 'atlas_name_12', 'cdl30_2001_5070', 'cdl30_2002_5070', 'cdl30_2003_5070', 'cdl30_2004_5070', 'cdl30_2005_5070', 'cdl56_2005_5070', 'cdl56_2006_5070', 'cdl30_2007_5070', 'cdl56_2007_5070', 'cdl30_2008_5070', 'cdl56_2008_5070', 'cdl56_2009_5070', 'cdl30_2010_5070', 'cdl30_2011_5070', 'cdl30_2012_5070', 'cdl30_2013_5070', 'cdl30_2014_5070', 'cdl30_2015_5070', 'cdl30_2016_5070', 'MapunitRaster_conus_10m', 'mirad_acea', 'prism_acea', 'cdl_2005', 'cdl_2007', 'cdl_2008']
        # fields = ['OBJECTID', 'POINT_X', 'POINT_Y', 'HUC8', 'cdl30_2001_5070', 'cdl30_2002_5070', 'cdl30_2003_5070', 'cdl30_2004_5070', 'cdl30_2005_5070', 'cdl56_2005_5070', 'cdl56_2006_5070', 'cdl30_2007_5070', 'cdl56_2007_5070', 'cdl30_2008_5070', 'cdl56_2008_5070', 'cdl56_2009_5070', 'cdl30_2010_5070', 'cdl30_2011_5070', 'cdl30_2012_5070', 'cdl30_2013_5070', 'cdl30_2014_5070', 'cdl30_2015_5070', 'cdl30_2016_5070', 'cdl_2005', 'cdl_2007', 'cdl_2008']

        print fields

        cond = "st_abbrev_1 = '"+state+"'"

        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(fc, fields, cond, skip_nulls = False, null_value = 0)
        print arr

        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        # use pandas method to import table into psotgres
        df.to_sql(fc, engine, schema='samples', if_exists='replace', index=False)

        #add trajectory field to table
        # addAcresField(fc, 'counts')


def createTable(state):
    cur = conn.cursor()
    
    query = """CREATE TABLE samples.samples_""" + state + """ as 
    SELECT 
      samples_merged_4152.objectid, 
      samples_merged_4152.area_m2_1 * 0.000247105 as acres, 
      samples_merged_4152.atlas_stco as fips, 
      samples_merged_4152.mlra_id as mlra, 
      samples_merged_4152.huc8, 
      samples_merged_4152.mukey_stgo as statsgo,
      samples_merged_4152.mapunitraster_conus_10m as ssurgo, 
      samples_merged_4152.mirad_acea as mirad, 
      samples_merged_4152.prism_acea as prism, 
      samples_merged_4152.cdl30_2001_5070 as cdl_2001, 
      samples_merged_4152.cdl30_2002_5070 as cdl_2002, 
      samples_merged_4152.cdl30_2003_5070 as cdl_2003, 
      samples_merged_4152.cdl30_2004_5070 as cdl_2004, 
      samples_merged_4152.cdl_2005 as cdl_2005, 
      samples_merged_4152.cdl56_2006_5070 as cdl_2006, 
      samples_merged_4152.cdl_2007 as cdl_2007, 
      samples_merged_4152.cdl_2008 as cdl_2008, 
      samples_merged_4152.cdl56_2009_5070 as cdl_2009, 
      samples_merged_4152.cdl30_2010_5070 as cdl_2010, 
      samples_merged_4152.cdl30_2011_5070 as cdl_2011, 
      samples_merged_4152.cdl30_2012_5070 as cdl_2012, 
      samples_merged_4152.cdl30_2013_5070 as cdl_2013, 
      samples_merged_4152.cdl30_2014_5070 as cdl_2014, 
      samples_merged_4152.cdl30_2015_5070 as cdl_2015, 
      samples_merged_4152.cdl30_2016_5070 as cdl_2016, 
      samples_merged_4152.point_x as lng, 
      samples_merged_4152.point_y as lat
    FROM 
      samples.samples_merged_4152;"""


    cur.execute(query)

    conn.commit()
    print "Records created successfully";
    # conn.close()


def concatenateCDLfields():
    with arcpy.da.UpdateCursor("D:\\projects\\ksu\\v2\\main\\samples.gdb\\samples_merged",["cdl30_2008_5070", "cdl56_2008_5070", "cdl_2008"]) as cursor:
        for row in cursor:
            a_value = row[0] if row[0] else 0  # Use 0 when "A" is falsy
            b_value = row[1] if row[1] else 0  # Use 0 when "B" is falsy
            row[2] = a_value + b_value
            cursor.updateRow(row)


def updateField():
    with arcpy.da.UpdateCursor("D:\\projects\\ksu\\v2\\main\\clu.gdb\\clu2008county_5070",["dataset"]) as cursor:
        for row in cursor:
            if row[0] == None:
                row[0] = 'clu'
                cursor.updateRow(row)


def exportPGtoCSV(state):
    cur = conn.cursor()
    sys.stdout = open('D:\\projects\\ksu\\v2\\samples_'+state+'.csv', 'w')
    

    cur.copy_expert("COPY samples.samples_"+state+" TO STDOUT WITH CSV HEADER", sys.stdout)
    
    conn.commit()
    print "Records created successfully";
    conn.close
# def formatTable():
#     # Set local variables
    
#     inFeatures = defineGDBpath(['v2','samples'])+'samples_merged_4152_az'
#     outLocation = defineGDBpath(['v2','samples'])
#     outFeatureClass = "samples_merged_4152_az_formatted"


#     # Create the necessary FieldMap and FieldMappings objects
#     fm = arcpy.FieldMap()
#     fms = arcpy.FieldMappings()


#     # Add the intersection field to the second FieldMap object
#     fm.addInputField(inFeatures, "atlas_st")

#     f_name = fm.outputField
#     f_name.name = 'yo'
#     fm.outputField = f_name

#     # Add both FieldMaps to the FieldMappings Object
#     fms.addFieldMap(fm)
    

#     # Execute FeatureClassToFeatureClass
#     arcpy.FeatureClassToFeatureClass_conversion(inFeatures, outLocation, outFeatureClass, field_mapping=fms)



    



####call functions
# addGDBTable2postgres()
# concatenateCDLfields()
# formatTable()
# updateField()


formatTables()









































# import arcpy import numpy as np import pandas as pd from pandas import DataFrame  
# #Create variable for feature class fc = r'C:\Projects\MyGeodatabase.gdb\Groundwater\WaterQuality'  
# #Create field list with a subset of the fields (cannot include datetime fields for  #da.FeatureClassToNumPy tool)
#  fc_fields = ['OBJECTID', 'WellID', 'Aquifer', 'FlowPeriod', 'As_D_Val','Cu_D_Val',              'GWElev', 'MeasuringPtElev', 'Total_depth', 'E', 'N']  
#  #Convert Feature Class to NumPy Array.  Due to the fact that NumPy arrays do not #accept null values for integer fields,
#   I had to convert null values to -99999 fc_np = arcpy.da.FeatureClassToNumPyArray(fc, fc_fields, skip_nulls = False,                                           null_value = -99999) 
#    #Convert NumPy array to pandas DataFrame.   fc_pd = DataFrame(fc_np)  








# NAD_1983_Contiguous_USA_Albers
# WKID: 5070 Authority: EPSG

# Projection: Albers
# False_Easting: 0.0
# False_Northing: 0.0
# Central_Meridian: -96.0
# Standard_Parallel_1: 29.5
# Standard_Parallel_2: 45.5
# Latitude_Of_Origin: 23.0
# Linear Unit: Meter (1.0)

# Geographic Coordinate System: GCS_North_American_1983
# Angular Unit: Degree (0.0174532925199433)
# Prime Meridian: Greenwich (0.0)
# Datum: D_North_American_1983
#   Spheroid: GRS_1980
#     Semimajor Axis: 6378137.0
#     Semiminor Axis: 6356752.314140356
#     Inverse Flattening: 298.257222101






# Albers_Conical_Equal_Area
# Authority: Custom

# Projection: Albers
# false_easting: 0.0
# false_northing: 0.0
# central_meridian: -96.0
# standard_parallel_1: 29.5
# standard_parallel_2: 45.5
# latitude_of_origin: 23.0
# Linear Unit: Meter (1.0)

# Geographic Coordinate System: GCS_North_American_1983
# Angular Unit: Degree (0.0174532925199433)
# Prime Meridian: Greenwich (0.0)
# Datum: D_North_American_1983
#   Spheroid: GRS_1980
#     Semimajor Axis: 6378137.0
#     Semiminor Axis: 6356752.314140356
#     Inverse Flattening: 298.257222101













# gdalwarp -overwrite -t_srs EPSG:5070 -of HFA D:/projects/ksu/v2/rasters/other/mirad_acea.tif D:/projects/ksu/v2/rasters/other/mirad_5070.img