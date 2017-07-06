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










# str( !VAT_cdl_2010.Class_Name! ) +' ('+ str(!VAT_bfc.acres!) + ')'





















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
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
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



def createMetaTable(state):
    os.chdir('C:/Users/Bougie/Desktop/Gibbs/Intact_land/SDSU_CLU_project/CLU Data/'+ state)

    for file in glob.glob("*.shp"):
        # print file
        fnf=(os.path.splitext(file)[0]).split("_")
        print fnf

        # # insert the values into
        # insertValuesPG(fnf)
        # #fnf=file name fragments
        # fnf=(os.path.splitext(fc)[0]).split("_")
        # print fnf

        cur = conn.cursor()
        query="INSERT INTO metadata.metatable VALUES (" + str(fnf[2]) + " , '" + str(fnf[4]) + "' , '" + str(fnf[5])+ "' , '" + str(file)+ "')"
        print query
        cur.execute(query)
        conn.commit()


def main_layers(gdb_path, wc, state, year):
    arcpy.env.workspace = defineGDBpath(gdb_path)
    featureclasses = arcpy.ListFeatureClasses(wc)
    print type(featureclasses)
  
    if len(featureclasses) >= 1:
 
        arcpy.Merge_management(featureclasses, defineGDBpath(['intact_land','merged'])+state+'_'+str(year))

    # arcpy.Dissolve_management(defineGDBpath(['intact_land','merged'])+'merged', defineGDBpath(['intact_land','merged'])+'merged_dissolved2', ["CLUCLSCD"], "", "SINGLE_PART", "DISSOLVE_LINES")

   

def createLayer(gdb_path, fc, layerlist):


    # Set overwrite option
    arcpy.env.overwriteOutput = True

    layer = 'layer_'+fc
    # Make a layer from the feature class
    arcpy.MakeFeatureLayer_management(defineGDBpath(gdb_path)+fc,layer)

    # # Within the selection (done above) further select only those cities that have a population >10,000
    arcpy.SelectLayerByAttribute_management(layer, "NEW_SELECTION", "CLUCLSCD = 2")

    addLayerToMXD(layer)

    layerlist.append(layer)


    # Execute Dissolve using LANDUSE and TAXCODE as Dissolve Fields
    # arcpy.Dissolve_management("temp_layer", defineGDBpath(['intact_land','merged'])+'try4', ["CLUCLSCD"], "", "MULTI_PART", "DISSOLVE_LINES")

    # # Write the selected features to a new featureclass
    # arcpy.CopyFeatures_management("temp_layer", defineGDBpath(['intact_land','merged'])+'try3')


def addLayerToMXD(layer):
    mxd = arcpy.mapping.MapDocument(r"C:/Users/Bougie/Desktop/Gibbs/arcgis/map_documents/temp_map.mxd")
    df = arcpy.mapping.ListDataFrames(mxd)[0]
    addlayer = arcpy.mapping.Layer(layer)
    arcpy.mapping.AddLayer(df, addlayer, "AUTO_ARRANGE")

    mxd.save()

    del mxd

        
def deleteLayers():
    mxd = arcpy.mapping.MapDocument(r"C:/Users/Bougie/Desktop/Gibbs/arcgis/map_documents/temp_map.mxd")
    for df in arcpy.mapping.ListDataFrames(mxd):
        for lyr in arcpy.mapping.ListLayers(mxd, "", df):
            arcpy.mapping.RemoveLayer(df, lyr)
    mxd.save()
    del mxd



######  call getRasterCount() function  ##############################

# years = range(2005, 2017)
# states = ['ia4']
# for state in states:
#     for year in years:
#         main_layers(['intact_land',state], '*_acea_'+str(year)+'*', state, year)
    # deleteLayers()


# arcpy.Project_management(clu_c_19001_15_20051206_20060228_shp, clu_c_19001_15_20051206_2006, "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "", "PROJCS['NAD_1983_UTM_Zone_15N',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-93.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")
























def ProjectSF(gdb_path, infc, outfc):
    arcpy.env.workspace = defineGDBpath(gdb_path)

    # run project tool
    arcpy.Project_management(infc, outfc, "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "", "PROJCS['NAD_1983_UTM_Zone_15N',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-93.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")




def executeQuery(query):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()   






def main():

    query='''SELECT 
             name
             FROM 
              metadata.metatable
             where pub_start NOT IN (select max(pub_start) from metadata.metatable group by fips, EXTRACT(YEAR FROM pub_start))
             order by fips, pub_start'''
    print query
    list_sf = g.fetchPG('intact_lands', query)

    for sf in list_sf:
        fnf=(os.path.splitext(sf)[0]).split("_")
        print fnf
        fnf[3]='acea'
        print fnf
        outfc = '_'.join(fnf)
        print outfc
        try:
            ProjectSF(['intact_land','ia5'], sf, outfc)
        except:
            print 'bad file:', sf
            e = sys.exc_info()[1]
            
            query="INSERT INTO metadata.corrupt VALUES ('" + str(sf) + "','"+str(fnf[2])  + "','"+str(e.args[0]) + "')"
            print query
            executeQuery(query)






# def insertValuesPG(fnf):


#     cur = conn.cursor()
#     query="INSERT INTO metadata.metatable VALUES (" + str(fnf[2]) + " , '" + str(fnf[4]) + "' , '" + str(fnf[5])+ "')"
#     print query
#     cur.execute(query)
#     conn.commit()







######################  call functions  ###############################################################
# createMetaTable('ia')
main()