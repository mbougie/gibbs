from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import geopandas as gpd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
import subprocess


try:
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



def create_CLU_ByYear():
    yearlist = range(2008,2016)
    # yearlist = [2005]
    
    
    for year in yearlist:
        ##create an array to hold all files from all state gdbs for a given year
        countylist= []
        
        print year

        arcpy.env.workspace = "D:\\projects\\intact_land\\states"

        # List all file geodatabases in the current workspace
        workspaces = arcpy.ListWorkspaces("*", "FileGDB")
        
        #get each state geodtabase
        for workspace in workspaces:
            print workspace
            arcpy.env.workspace = workspace
            
            ##list features for a given year in each state geodatabase
            featureclasses = arcpy.ListFeatureClasses("*_acea_{}*".format(str(year)))
            for fc in featureclasses:
                print 'fc', fc
                countylist.append("{}//{}".format(workspace,fc))
        
        print "number of files for {}: {}".format(str(year), len(countylist))
        if len(countylist) > 0:
            ##create geodatabase for each year
            arcpy.CreateFileGDB_management("D:\\projects\\intact_land\\years\\", "{}.gdb".format(str(year)))
            ##mosiac the files contained in the countylistper year
            arcpy.Merge_management(countylist, "D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}".format(str(year), str(year)))





def create_Crop_byYear():
    yearlist = range(2010,2016)

    ##create an array to hold all files from all state gdbs for a given year
    for year in yearlist:
        print 'current year:', year
        
        ##define current working geodatabase
        gdb = "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year))
        
        arcpy.env.workspace = gdb
        
        ##define variables
        fc = 'clu_{}'.format(str(year))
        layer = '{}_crop'.format(fc)
        clause_list = ['CLUCLSCD=2', 'CROPLND3CM=1']

        ##function that selects columns if they exist for a dataset
        def createWhereClause(fc):
            field_list = [f.name for f in arcpy.ListFields(fc)]
            print field_list

            if 'CROPLND3CM' in field_list:
                clause = ' OR '.join(clause_list)
                return clause

            else:
                return clause_list[0]
        
        ##perform processing only if feature class exists
        if arcpy.Exists(fc):
            ##create layer using WHERE clause
            arcpy.MakeFeatureLayer_management(fc, layer, createWhereClause(fc))
            ##----create the crop dataset per year-----------------------------------------------------------------
            arcpy.FeatureClassToFeatureClass_conversion(layer, gdb, layer)
            ##----repair geometry
            # arcpy.RepairGeometry_management(layer)
            
            ##----create the yearly cummalative crop dataset-------------------------------------------------------
            if year == 2004:
                fc_prev = "D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_crop".format(str(year-1), str(year-1))
                arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_{}_crop_cumm'.format(str(year)))
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))

            elif year == 2011:
                fc_2009 = "D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_crop_cumm".format(str(2009), str(2009))
                arcpy.Union_analysis(in_features=[fc_2009,layer], out_feature_class='clu_{}_crop_cumm'.format(str(year)))
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))
            elif year > 2004 & year != 2011:
                fc_prev = "D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_crop_cumm".format(str(year-1), str(year-1))
                arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_{}_crop_cumm'.format(str(year)))
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))




def create_NonCrop_byYear():
    yearlist = range(2008,2015)
    
    ##create an array to hold all files from all state gdbs for a given year
    
    for year in yearlist:
        print year
        countylist= []

        arcpy.env.workspace = "D:\\projects\\intact_land\\states"

        # List all file geodatabases in the current workspace
        workspaces = arcpy.ListWorkspaces("*", "FileGDB")
        
        #get each state geodtabase
        for workspace in workspaces:
            print workspace
            arcpy.env.workspace = workspace
            
            ##list features for a given year in each state geodatabase
            featureclasses = arcpy.ListFeatureClasses("*_acea_{}*".format(str(year)))

            for fc in featureclasses:
                print 'fc:', fc
                substring_list=fc.split("_")
                countylist.append("'"+substring_list[2]+"'")
        
        print "number of counties for {}: {}".format(str(year), len(countylist))

        def createWhereString(countylist):
            print countylist
            cntyString = ' OR atlas_stco='.join(countylist)
            cond='atlas_stco={}'.format(cntyString)
            return cond


        if len(countylist) > 0:
            in_features = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties'
            layer = 'counties_{}'.format(str(year))
            where_clause = createWhereString(countylist)
            
            # # # Make a layer from the feature class
            arcpy.MakeFeatureLayer_management(in_features, layer, where_clause)
            
            #create a feature class containing a counties with data for a given year
            arcpy.FeatureClassToFeatureClass_conversion(layer, "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year)), "clu_{}_counties".format(str(year)))
            
            #create a dissolved feature class containing a counties with data for a given year---i.e. create a one record file
            # arcpy.Dissolve_management(in_features=layer, out_feature_class="D:\\projects\\intact_land\\years\\{}.gdb\\counties_{}_dissolved".format(str(year),str(year)), dissolve_field=["entity"])

            ##------------create to noncrop dataset per year ---------------------------------------------------------------------------
            arcpy.Erase_analysis(in_features=layer, erase_features="D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_crop".format(str(year),str(year)), out_feature_class="D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_noncrop".format(str(year),str(year)))
           

def create_NonCrop_byYear_cumulative():
    # yearlist = [2015]
    yearlist = range(2004,2016)
    
    ##create an array to hold all files from all state gdbs for a given year
    # countylist= []
    for year in yearlist:
        print year
        countylist= []


        arcpy.env.workspace = "D:\\projects\\intact_land\\states"

        # List all file geodatabases in the current workspace
        workspaces = arcpy.ListWorkspaces("*", "FileGDB")
        
        #get each state geodtabase
        for workspace in workspaces:
            # print 'workspace-----', workspace
            arcpy.env.workspace = workspace

            countyyears = range(2003,year+1)
            # print countyyears
            for year in countyyears:

                featureclasses = arcpy.ListFeatureClasses("*_acea_{}*".format(str(year)))

                for fc in featureclasses:
                    substring_list=fc.split("_")
                    countylist.append("'"+substring_list[2]+"'")
        
  
        print "number of counties {}".format(len(countylist))
        print "number of counties set {}".format(len(set(countylist)))


        countyset = set(countylist)
        countylist = list(set(countylist))
    
        def createWhereString(countylist):
            print countylist
            cntyString = ' OR atlas_stco='.join(countylist)
            cond='atlas_stco={}'.format(cntyString)
            return cond

        if year != 2010:
            in_features = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties'
            layer = 'clu_{}_counties_c'.format(str(year))
            where_clause = createWhereString(countyset)
            
            ### Make a layer from the feature class
            arcpy.MakeFeatureLayer_management(in_features, layer, where_clause)
         
            ### create a feature class from layer containing a counties with data for a given year
            # arcpy.FeatureClassToFeatureClass_conversion(layer, "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year)), 'clu_{}_counties_c'.format(str(year)))

             ##------------create cummulaitve noncrop dataset per year
            #arcpy.Erase_analysis(in_features=layer, erase_features="D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_crop_c".format(str(year),str(year)), out_feature_class="D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_noncrop_c".format(str(year),str(year)))


        

def gdbToPG():
    yearlist = range(2006,2008)
    for year in yearlist:
        if year != 2010:
            # subprocess.call(' ogr2ogr -f "PostgreSQL" PG:"host=144.92.235.105 port=5432 dbname=intact_lands schemas=clu_{0} user=mbougie password=Mend0ta!" D:\\projects\\intact_land\\years\\{0}.gdb -nlt PROMOTE_TO_MULTI -nln clu_{0}.clu_{0}_counties clu_{0}_counties -overwrite'.format(str(year)), shell=True)

            alterTable(year)



def alterTable(year):
    cur = conn.cursor()

    query = 'ALTER TABLE clu_{0}.clu_{0}_counties ADD COLUMN year integer; UPDATE clu_{0}.clu_{0}_counties set year={0}'.format(str(year))
    print query
    cur.execute(query)
    conn.commit()



def unionTables():
    yearlist = range(2003,2016)
    querylist = ["CREATE TABLE public.testunion2 AS SELECT atlas_stco,year FROM clu_2003.clu_2003_counties,spatial.states WHERE substring(atlas_stco from 1 for 2) = atlas_st"]
    for year in yearlist:
        if year != 2010:
            cur = conn.cursor()
            
            query = 'SELECT atlas_stco,year FROM clu_{0}.clu_{0}_counties,spatial.states WHERE substring(atlas_stco from 1 for 2) = atlas_st'.format(str(year))
            print query
            querylist.append(query)

    print(querylist)
    queryfinal = ' UNION '.join(querylist)
    print queryfinal
    cur.execute(queryfinal)
    conn.commit()



######################  call functions  ###############################################################
######--- creation of CLU by year code  ------
# create_CLU_ByYear()

######--- crop code  ------
# create_Crop_byYear()

######--- noncrop -----
# create_NonCrop_byYear()
create_NonCrop_byYear_cumulative()






### describe data  ###################
# gdbToPG()
#unionTables()






































#===========================================================================
#============  metadata functions  =========================================
#===========================================================================

#__________create the metatable for all states
# createMetaTable()

#_________  fill out  ____________________________________
# popCounts_years()



#===========================================================================
#============  processing functions  =======================================
#===========================================================================

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
# mergeAllyearsAllstates()




























