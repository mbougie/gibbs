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
    ##Description: goes through all the states gdbs and selects the files with year from yearlist.  Merges all datasets of a given year and stores dataset in [year].gdb
    
    yearlist = range(2014,2016)

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
    ## Description: go through each [year].gdb and select only features that fit the crop condition (i.e. 'CLUCLSCD=2' OR 'CROPLND3CM=1') store this as clu_[year]_crop.
    ## Add this clu_[year]_crop to the pervious years cropped dataset to create the cumulative dataset.

    yearlist = range(2014,2016)

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
                print 'clause:', clause
                return clause
            else:
                print 'clause:', clause_list[0]
                return clause_list[0]
        
        ##perform processing only if feature class exists
        if arcpy.Exists(fc):
            print('---dataset exists---')
            ##create layer using WHERE clause
            arcpy.MakeFeatureLayer_management(fc, layer, createWhereClause(fc))
           
            ##----create the crop dataset per year-----------------------------------------------------------------
            arcpy.FeatureClassToFeatureClass_conversion(layer, gdb, layer)
     
            
            ##----create the yearly cummalative crop dataset-------------------------------------------------------
            if year == 2004:
                fc_prev = "D:\\projects\\intact_land\\years\\2003.gdb\\clu_2003_crop"
                arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_2004_crop_c')
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))

            elif year == 2011:
                fc_2009 = "D:\\projects\\intact_land\\years\\2009.gdb\\clu_2009_crop_c"
                arcpy.Union_analysis(in_features=[fc_2009,layer], out_feature_class='clu_2011_crop_c')
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))
            elif year > 2004 & year != 2011:
                fc_prev = "D:\\projects\\intact_land\\years\\{0}.gdb\\clu_{0}_crop_c".format(str(year-1))
                arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_{}_crop_c'.format(str(year)))
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))




def create_NonCrop_byYear():
    ##Description: merge all the couties for a given year and then use erase() function with clu_[year]_crop to get non crop dataset


    yearlist = range(2014,2016)
    
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
            
            ##------------create to noncrop dataset per year ---------------------------------------------------------------------------
            arcpy.Erase_analysis(in_features=layer, erase_features="D:\\projects\\intact_land\\years\\{0}.gdb\\clu_{0}_crop".format(str(year)), out_feature_class="D:\\projects\\intact_land\\years\\{0}.gdb\\clu_{0}_noncrop".format(str(year)))
           

def create_NonCrop_byYear_cumulative():
    # yearlist = [2015]
    yearlist = range(2015,2016)
    
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
         
            ### create a feature class from layer containing counties with data for a given year
            # arcpy.FeatureClassToFeatureClass_conversion(layer, "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year)), 'clu_{}_counties_c'.format(str(year)))
            
            

             ##------------create cummulaitve noncrop dataset per year
            # arcpy.Erase_analysis(in_features=layer, erase_features="D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_crop_c".format(str(year),str(year)), out_feature_class="D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}_noncrop_c".format(str(year),str(year)))


            arcpy.Dissolve_management(in_features="D:\\projects\\intact_land\\years\\{0}.gdb\\clu_{0}_noncrop_c".format(str(year)), out_feature_class="D:\\projects\\intact_land\\years\\{0}.gdb\\clu_{0}_noncrop_c_dissolved".format(str(year)), dissolve_field=["entity"])




def calcGeom(table_name, year):
    arcpy.env.workspace = "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year))
    field_name = 'acres_{}_c'.format(str(year))
    arcpy.AddGeometryAttributes_management(Input_Features=table_name, Geometry_Properties="AREA", Area_Unit="ACRES")   


        

def gdbToPG(table_suffix):
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')
    
    yearlist = range(2004,2011)
    for year in yearlist:
        arcpy.env.workspace = "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year))
        table_name = 'clu_{0}_{1}'.format(str(year),table_suffix)
        if year != 2010:
            print('year:', year)
            # calcGeom(table_name, year)
            # subprocess.call(' ogr2ogr -f "PostgreSQL" PG:"host=144.92.235.105 port=5432 dbname=intact_lands schemas=clu_{0} user=mbougie password=Mend0ta!" D:\\projects\\intact_land\\years\\{0}.gdb -nlt PROMOTE_TO_MULTI -nln clu_{0}.clu_{0}_{1} clu_{0}_{1} -overwrite'.format(str(year), table_suffix), shell=True)
            
            arr = arcpy.da.FeatureClassToNumPyArray(table_name, ('atlas_stco', 'POLY_AREA'))
            print(arr)

            # convert numpy array to pandas dataframe
            df = pd.DataFrame(data=arr)
            df.columns = map(str.lower, df.columns)

            # use pandas method to import table into psotgres
            df.to_sql(table_name, engine, schema='clu_{}'.format(str(year)))
            alterTable(year)



def alterTable(year):
    cur = conn.cursor()

    query = 'ALTER TABLE clu_{0}.clu_{0}_noncrop_c ADD COLUMN year integer; UPDATE clu_{0}.clu_{0}_noncrop_c set year={0}'.format(str(year))
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
# create_NonCrop_byYear_cumulative()






### describe data  ###################
# gdbToPG('noncrop_c')
#unionTables()

import arcpy
from arcpy import env
from arcpy.sa import *

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")


env.workspace = "D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\compare.gdb"
in_raster1 = Raster('swift_clu_2015_noncrop_c_w_masks_raster_zero')
in_raster2 = Raster('swift_nlcd_intact_modified_zero')
outraster = 'D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\compare.gdb\\swift_clu_2015_noncrop_c_w_masks_raster_zero'

# outCon2 = Con(IsNull(in_raster), 0, 1)
# outCon2.save(outraster)


outCombine = Combine([in_raster1, in_raster2])
outCombine.save("comb")





































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




























