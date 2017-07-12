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



def createMetaTable():
    states = getDirectories()
    for state in states:
        if len(state) == 2:
            #only want to process the main directories with 2 letters that represnt the to letters of a state
            print state
        
            os.chdir('C:/Users/Bougie/Desktop/Gibbs/Intact_land/SDSU_CLU_project/CLU Data/'+ state)
            

            for file in glob.glob("*.shp"):
                # print file
                fnf=(os.path.splitext(file)[0]).split("_")
                print fnf

                cur = conn.cursor()
                query="INSERT INTO metadata.metatable VALUES ('" + state + "' , '" + str(fnf[2])  + "' , '" + str(fnf[4]) + "' , '" + str(fnf[5])+ "' , '" + str(file)+ "')"
                print query
                cur.execute(query)
                conn.commit()




def mergeFC(gdb_path, wc, state, year):
    arcpy.env.workspace = defineGDBpath(gdb_path)
    featureclasses = arcpy.ListFeatureClasses(wc)
    print type(featureclasses)
  
    if len(featureclasses) >= 1:
 
        arcpy.Merge_management(featureclasses, defineGDBpath(['intact_land',state+'_merged'])+state+'_'+str(year))


def addFields(gdb_path, state, year): 
    print gdb_path
    print year
    arcpy.env.workspace = defineGDBpath(gdb_path)

    featureclasses = arcpy.ListFeatureClasses(wild_card = '*'+str(year))

    # Copy shapefiles to a file geodatabase
    for fc in featureclasses:

        print fc
     
        # Execute AddField twice for two new fields
        # arcpy.AddField_management(fc, 'state', "TEXT")
        
        #fill column with uniform value
        addValuetoColumn(fc, 'state', state)
        
        # arcpy.AddField_management(fc, 'year', "SHORT", field_length=5)

        #fill column with uniform value
        addValuetoColumn(fc, 'year', year)

def addValuetoColumn(fc, column_name, value):  
    cur = arcpy.UpdateCursor(fc)

    for row in cur:
        row.setValue(column_name, value)
        cur.updateRow(row)

def ProjectSF(state, infc, outfc):
    os.chdir('C:/Users/Bougie/Desktop/Gibbs/Intact_land/SDSU_CLU_project/CLU Data/'+ state)
    outfc_path = defineGDBpath(['intact_land',state]) + outfc

    # run project tool
    arcpy.Project_management(infc, outfc_path, "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "", "PROJCS['NAD_1983_UTM_Zone_15N',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-93.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")




def executeQuery(query):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()   






def mainProjectSF():

    states = getDirectories()
    for state in states:
        if len(state) == 2 and state == 'mn':

            #variable with quotes
            vwq = "'" + state + "'"

            query="""SELECT 
                    entire.name
                    FROM  
                    metadata.metatable as entire RIGHT OUTER JOIN(SELECT max(pub_start) as pub_start,fips FROM metadata.metatable GROUP BY fips, EXTRACT(YEAR FROM pub_start)) as subset 
                    ON entire.pub_start = subset.pub_start AND entire.fips = subset.fips
                    WHERE state = """ + vwq

            print query
            list_sf = g.fetchPG('intact_lands', query)
            print list_sf
            print len(list_sf)

            for sf in list_sf:
                fnf=(os.path.splitext(sf)[0]).split("_")
                print fnf
                fnf[3]='acea'
                print fnf
                outfc = '_'.join(fnf)
                print outfc
                try:
                    ProjectSF(state, sf, outfc)
                except:
                    print 'bad file:', sf
                    e = sys.exc_info()[1]
                    
                    query="INSERT INTO metadata.corrupt VALUES ('" + str(sf) + "','"+str(fnf[2])  + "','"+str(e.args[0]) + "')"
                    print query
                    executeQuery(query)



def applyCond2FC(gdb_path, fc, expression, fc_out):
 
    fc_in = defineGDBpath(gdb_path) + fc
    print fc_in
    out_path = defineGDBpath(gdb_path)

    arcpy.FeatureClassToFeatureClass_conversion(fc_in, out_path, fc_out, expression) 



def FieldExist(gdb_path, fc, fieldname):
    arcpy.env.workspace = defineGDBpath(gdb_path)
    fieldList = arcpy.ListFields(fc, fieldname)

    fieldCount = len(fieldList)

    if (fieldCount == 1):
        if fieldname == "CLUCLSCD":
            expression1 = "CLUCLSCD = 2"
            fc_out1 = fc + '_clscd_2'
            applyCond2FC(gdb_path, fc, expression1, fc_out1)
            expression2 = "CLUCLSCD <> 2"
            fc_out2 = fc + '_clscd_not2'
            applyCond2FC(gdb_path, fc, expression2, fc_out2)
        if fieldname == "CROPLND3CM":
            expression1 = "CROPLND3CM = 1"
            fc_out1 = fc + '_3cm_1'
            applyCond2FC(gdb_path, fc, expression1, fc_out1)
            expression2 = "CROPLND3CM <> 1"
            fc_out2 = fc + '_3cm_not1'
            applyCond2FC(gdb_path, fc, expression2, fc_out2)






def mainSubsetFC():
    states = getDirectories()
    for state in states:
        if len(state) == 2 and state == 'mn':
            arcpy.env.workspace = defineGDBpath(['intact_land',state+'_merged'])
            featureclasses = arcpy.ListFeatureClasses()
            for fc in featureclasses:
                print fc
                fieldnames = ['CLUCLSCD','CROPLND3CM']
                for field in fieldnames:
                    FieldExist(['intact_land',state+'_merged'], fc, field)



def getDirectories():
    os.chdir('C:/Users/Bougie/Desktop/Gibbs/Intact_land/SDSU_CLU_project/CLU Data/')
    dirlist = glob.glob("*")
    return dirlist





def mainMergeFC():
    #hard coded range of years of all datasets in clu
    years = range(2003,2016)
    states = getDirectories()
    for state in states:
        if len(state) == 2 and state == 'mn':
            #only want to process the main directories with 2 letters that represnt the to letters of a state
            print state

            for year in years:
                # mergeFC(['intact_land',state], '*_acea_'+str(year)+'*', state, year)
                addFields(['intact_land',state+'_merged'], state, year)
    






def mergeSubsetsbyState():
    subsetlist = ['_clscd_2','_clscd_not2','_3cm_1','_3cm_not1']
    states = getDirectories()
    for state in states:
        if len(state) == 2 and state == 'mn':
            for subset in subsetlist:
                wc = state + '*' + subset
                #only want to process the main directories with 2 letters that represnt the to letters of a state
                print wc  
                arcpy.env.workspace = defineGDBpath(['intact_land','merged'])
                featureclasses = arcpy.ListFeatureClasses(wc)
                print featureclasses
                arcpy.Merge_management(featureclasses, state+subset)



######################  call functions  ###############################################################


#__________create the metatable for all states
# createMetaTable()


#__________reproject sf and store as fc in geodatabase
# mainProjectSF()



#________  create the base merged dataset by state and year _____________________________________________________________
mainMergeFC()



#_________  create the merged subset feature classes  ____________________________________
# mainSubsetFC()


# mergeSubsetsbyState()





