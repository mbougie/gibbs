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



coef={'acres':1,'msq':0.000247105,'30m':0.222395,'56m':0.774922476}


#import extension
arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.CheckOutExtension("Spatial")



###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/Bougie/Desktop/Gibbs/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 



#============  metadata functions  =========================================

def createMetaTable():
    query1="DELETE FROM metadata.corrupt"
    print query1
    executeQuery(query1)

    query2="DELETE FROM metadata.clu"
    print query2
    executeQuery(query2)


    states = getDirectories()
    for state in states:
        if len(state) == 2:
            #only want to process the main directories with 2 letters that represnt the to letters of a state
            print state
        
            os.chdir('C:/Users/Bougie/Desktop/Gibbs/Intact_land/SDSU_CLU_project/CLU Data/'+ state)
            

            for sf in glob.glob("*.shp"):
                print sf
                try:
                    result = arcpy.GetCount_management(sf)
                    count = int(result.getOutput(0))
                    print(count)

                    fnf=(os.path.splitext(sf)[0]).split("_")
                    print fnf

                    cur = conn.cursor()
                    query="INSERT INTO metadata.clu VALUES ('" + state + "' , '" + str(fnf[2])  + "' , '" + str(fnf[4]) + "' , '" + str(fnf[5])+ "' , " + str(count)+ " , '" + str(file)+ "')"
                    print query
                    cur.execute(query)
                    conn.commit()


                except:
                    print 'corupted---------------', sf
                    e = sys.exc_info()[1]
                    
                    query="INSERT INTO metadata.corrupt VALUES ('" + str(sf) + "','"+str(fnf[2])  + "','"+str(e.args[0]) + "')"
                    print query
                    executeQuery(query)

# NOTE: incorportate this !!!!
# update
#   metadata.metatable set year = EXTRACT(YEAR FROM pub_start)



#============  processing functions  =======================================

def popCounts_years():
    years = range(2003,2016)

    query="SELECT state FROM metadata.counts GROUP BY state"
    print query
    states = g.fetchPG('intact_lands', query)
     

    for year in years:
        for state in states:
            vwq = "'" + state + "'"
            query = "UPDATE metadata.counties_years SET clu_"+str(year)+" = (SELECT count FROM metadata.counts WHERE state = "+vwq+" and year = "+str(year)+") WHERE state = "+vwq
            print query
            executeQuery(query)







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
    # reproject to projection acea and determine the max date sf for a given year and state and store as fc in geodatabase
    states = getDirectories()
    for state in states:
        if len(state) == 2:

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
    # apply the condtion created based on if the column exists to the arcpy.FeatureClassToFeatureClass_conversion() function
    fc_in = defineGDBpath(gdb_path) + fc
    print fc_in
    out_path = defineGDBpath(gdb_path)

    arcpy.FeatureClassToFeatureClass_conversion(fc_in, out_path, fc_out, expression) 



def createCond(gdb_path, fc, fieldname):
    #create the condition for the arcpy.FeatureClassToFeatureClass_conversion() function based on if the column exists
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
    #description:subset the merged year by state by using the fileds: 'CLUCLSCD','CROPLND3CM'  
    states = getDirectories()
    for state in states:
        if len(state) == 2:
            arcpy.env.workspace = defineGDBpath(['intact_land', 'merged'])
            featureclasses = arcpy.ListFeatureClasses()
            for fc in featureclasses:
                print fc
                fieldnames = ['CLUCLSCD','CROPLND3CM']
                for field in fieldnames:
                    createCond(['intact_land', 'merged'], fc, field)



def getDirectories():
    os.chdir('C:/Users/Bougie/Desktop/Gibbs/Intact_land/SDSU_CLU_project/CLU Data/')
    dirlist = glob.glob("*")
    return dirlist



def mergeFC(gdb_path, wc, state, year):
    #description:merge the same year of a state's feature classes together
    arcpy.env.workspace = defineGDBpath(gdb_path)
    featureclasses = arcpy.ListFeatureClasses(wc)
    print featureclasses
  
    if len(featureclasses) >= 1:
 
        arcpy.Merge_management(featureclasses, defineGDBpath(['intact_land','merged'])+state+'_'+str(year))



def addFields(gdb_path, state, year):
#description: added the year field to all records so can destinguish when merge allyears and allstates_allyears 
    print gdb_path
    print year
    arcpy.env.workspace = defineGDBpath(gdb_path)

    featureclasses = arcpy.ListFeatureClasses(wild_card = state+'_'+str(year))
    print 'featureclasses to add year field to:', featureclasses

    # Copy shapefiles to a file geodatabase
    for fc in featureclasses:

        print 'fc:', fc
        arcpy.AddField_management(fc, 'YEAR', "SHORT", field_length=5)

        #fill column with uniform value
        addValuetoColumn(fc, 'YEAR', year)



def mainMergeFC():
    #hard coded range of years of all datasets in clu
    years = range(2003,2016)
    states = getDirectories()
    for state in states:
        if len(state) == 2:
            #only want to process the main directories with 2 letters that represnt the to letters of a state
            print state, '----------------------------------------------------------------------------------'

            for year in years:
                print year
                mergeFC(['intact_land',state], '*_acea_'+str(year)+'*', state, year)
                addFields(['intact_land','merged'], state, year)
    






def mergeAllyearsByState():
    subsetlist = ['clscd_2','clscd_not2','3cm_1','3cm_not1']
    # subsetlist = ['clscd_2','clscd_not2']
    states = getDirectories()
    # states = ['mt']
    for state in states:
        #only want to process the main directories with 2 letters that represnt the to letters of a state
        if len(state) == 2:
            for subset in subsetlist:
                wc = state + '*' + subset
                
                print wc  
                arcpy.env.workspace = defineGDBpath(['intact_land','merged'])
                featureclasses = arcpy.ListFeatureClasses(wc)
                print featureclasses
                arcpy.Merge_management(featureclasses, state+'_allyears_'+subset)




def changeCRPvaluesMT():
    #description: change the datatype of the crp column in early years in MT to integer so can merge this column with other states
    arcpy.env.workspace = defineGDBpath(['intact_land','merged'])
    wc = 'mt_allyears_clscd_*'
    featureclasses = arcpy.ListFeatureClasses(wc)
    print featureclasses

    for fc in featureclasses:
        # step1---Change name of crp field
        arcpy.AlterField_management(fc, "CRP", new_field_name="CRP_TEXT")

        #step2---add new field with datatpe integer 
        arcpy.AddField_management(fc, 'CRP', "SHORT")

        #step3---populate the new crp field by referencing the intial crp column
        cur = arcpy.UpdateCursor(fc)

        for row in cur:
            if row.getValue("CRP_TEXT") == 'CRP':
                row.setValue("CRP", 1)
                cur.updateRow(row)
            else:
                row.setValue("CRP", 0)
                cur.updateRow(row)





def mergeAllyearsAllstates():
    subsetlist = ['clscd_2','clscd_not2','3cm_1','3cm_not1']
    for subset in subsetlist:
        wc = '*allyears_' + subset
        print wc  
        arcpy.env.workspace = defineGDBpath(['intact_land','merged'])
        featureclasses = arcpy.ListFeatureClasses(wc)
        print featureclasses
        arcpy.Merge_management(featureclasses, 'allstates_allyears_'+subset)










######################  call functions  ###############################################################

#============  metadata functions  =========================================
#__________create the metatable for all states
# createMetaTable()

#_________  fill out  ____________________________________
# popCounts_years()

#============  processing functions  =======================================

#__________reproject the max date sf for a given year and state and store as fc in geodatabase
# mainProjectSF()


#________  merge datasets by state and year _____________________________________________________________
# mainMergeFC()


#_________  create the merged subset feature classes  ____________________________________
# mainSubsetFC()


#_________  fill out  ____________________________________
# mergeAllyearsByState()


#_________  fill out  ____________________________________
# changeCRPvaluesMT()


#_________  fill out  ____________________________________
mergeAllyearsAllstates()

























