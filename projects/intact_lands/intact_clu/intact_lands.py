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

sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen


try:
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"





def selectColumns(dataset):
    fieldmappings = arcpy.FieldMappings()



    # Add all fields from inputs.

    fieldmappings.addTable(dataset)

    # Name fields you want. Could get these names programmatically too.

    keepers = ["ATLAS_STCO"] # etc.



    # Remove all output fields you don't want.

    for field in fieldmappings.fields:

        if field.name not in keepers:

            fieldmappings.removeFieldMap(fieldmappings.findFieldMapIndex(field.name))








def create_fp_ByYear():
    ##Description: goes through all the states gdbs and selects the files with year from yearlist.  Merges all datasets of a given year and stores dataset in [year].gdb
    
    yearlist = range(2011,2016)
    # yearlist = [2004]

    for year in yearlist:
        ##create an array to hold all files from all state gdbs for a given year
        countylist= []
        
        print year

        arcpy.env.workspace = "D:\\projects\\intact_land\\states"

        # # List all file geodatabases in the current workspace
        # workspaces = arcpy.ListWorkspaces("*", "FileGDB")
        
        # #get each state geodtabase
        # for workspace in workspaces:
        #     print workspace
        #     arcpy.env.workspace = workspace
            
        #     ##list features for a given year in each state geodatabase
        #     featureclasses = arcpy.ListFeatureClasses("*_acea_{}*".format(str(year)))
        #     for fc in featureclasses:
        #         print 'fc', fc
        #         countylist.append("{}//{}".format(workspace,fc))
        
        # print "number of files for {}: {}".format(str(year), len(countylist))
        # if len(countylist) > 0:
            ##create geodatabase for each year
            # arcpy.CreateFileGDB_management("D:\\projects\\intact_land\\years\\", "{}.gdb".format(str(year)))
            ##mosiac the files contained in the countylistper year
            # arcpy.Merge_management(countylist, "D:\\projects\\intact_land\\years\\{}.gdb\\clu_{}".format(str(year), str(year)))

            # Set the workspace environment to local file geodatabase

        #### add atlas_stco column to dataset ###########################
        # arcpy.env.workspace = 'D:\\projects\\intactland\\intact_clu\\main\\years\\{0}.gdb'.format(str(year))

        # #####select only atlas_stco column
        # fc_counties='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties'
        # # selectColumns(dataset=fc_counties)
        # if year not in [2003,2010]:
        #     arcpy.SpatialJoin_analysis (target_features='clu_{0}'.format(str(year)), join_features=fc_counties, out_feature_class='clu_{0}'.format(str(year)), join_operation='JOIN_ONE_TO_ONE', join_type='KEEP_ALL', match_option='COMPLETELY_WITHIN')
 

        ###step2: create the cummulative dataset for this year##########################################
        os.chdir('G:\\ancillary_storage\\intactland\\intact_clu\\main\\years')

        # if year == 2004:
        #     fc_prev = "2003.gdb\\clu_2003"
        #     arcpy.Union_analysis(in_features=[fc_prev,'2004.gdb\\clu_2004'], out_feature_class='2004.gdb\\clu_2004_c')
        #     ##----repair geometry
        #     # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))

        # elif year == 2011:
        #     fc_2009 = "2009.gdb\\clu_2009_c"
        #     arcpy.Union_analysis(in_features=[fc_2009,'2011.gdb\\clu_2011'], out_feature_class='2011.gdb\\clu_2011_c')


        if year == 2012:
            fc_prev = "2011.gdb\\fp_2011"
            arcpy.Union_analysis(in_features=[fc_prev,'2012.gdb\\fp_2012'], out_feature_class='2012.gdb\\fp_2012_c')

            ##----repair geometry
            # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))
        elif year not in [2011,2012]:
            fc_prev = "{0}.gdb\\fp_{0}_c".format(str(year-1))
            arcpy.Union_analysis(in_features=[fc_prev,'{0}.gdb\\fp_{0}'.format(str(year))], out_feature_class='{0}.gdb\\fp_{0}_c'.format(str(year)))







def create_yo_byYear():
    ## Description: go through each [year].gdb and select only features that fit the crop condition (i.e. 'CLUCLSCD=2' OR 'CROPLND3CM=1') store this as clu_[year]_crop.
    ## Add this clu_[year]_crop to the pervious years cropped dataset to create the cumulative dataset.

    yearlist = range(2011,2016)
    # yearlist = [2009]
    ##create an array to hold all files from all state gdbs for a given year
    for year in yearlist:
        print 'current year:', year
        
        ##define current working geodatabase
        gdb = "G:\\ancillary_storage\\intactland\\intact_clu\\main\\years\\{}.gdb".format(str(year))
        
        arcpy.env.workspace = gdb
        
        ##define variables
        fc = 'clu_{}'.format(str(year))
        layer = '{}_fp'.format(fc)
        clause_list = ['CLUCLSCD<>2', 'CROPLND3CM<>1']
        

        ##function that selects columns if they exist for a dataset
        def createWhereClause(fc):
            field_list = [f.name for f in arcpy.ListFields(fc)]
            print field_list

            if 'CROPLND3CM' in field_list:
                clause = ' AND '.join(clause_list)
                print 'clause:', clause
                return clause
            else:
                print 'clause:', clause_list[0]
                return clause_list[0]
        
        ##perform processing only if feature class exists
        if arcpy.Exists(fc):
            print('---dataset exists---')
            #create layer using WHERE clause
            arcpy.MakeFeatureLayer_management(fc, layer, createWhereClause(fc))
           
            #----create the crop dataset per year-----------------------------------------------------------------
            arcpy.FeatureClassToFeatureClass_conversion(layer, gdb, layer)
     
            
            ####step2: create the cummulative dataset for this year##########################################
            os.chdir('G:\\ancillary_storage\\intactland\\intact_clu\\main\\years')

            '''Note: TWO steps (refine and extend) for creating fp cumulative footprint datasets.  First have to REFINE the previuos cumulative footprint to get rid of any areas that where converted
               to crop in the current year.  Then need to EXTEND the footprint to delineate new pathces regional that come from the current year'''

            # if year == 2004:
            #     fc_prev = "2003.gdb\\clu_2003_fp"
            #     arcpy.Erase_analysis(in_features=fc_prev,erase_features='2004.gdb\\clu_2004_crop', out_feature_class='2004.gdb\\clu_2004_fp_r')
            #     arcpy.Union_analysis(in_features=['2004.gdb\\clu_2004_fp_r','2004.gdb\\clu_2004_fp'], out_feature_class='clu_2004_fp_r_c')


            if year == 2012:
                fc_prev = "2011.gdb\\clu_2011_fp"
                arcpy.Erase_analysis(in_features=fc_prev,erase_features='2012.gdb\\clu_2012_crop', out_feature_class='2012.gdb\\clu_2012_fp_r')
                arcpy.Union_analysis(in_features=['2012.gdb\\clu_2012_fp_r','2012.gdb\\clu_2012_fp'], out_feature_class='clu_2012_fp_r_c')

                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))

            # elif year == 2011:
            #     fc_prev = "2009.gdb\\clu_2009_fp_r_c"
            #     arcpy.Erase_analysis(in_features=fc_prev,erase_features='2011.gdb\\clu_2011_crop', out_feature_class='2011.gdb\\clu_2011_fp_r')
            #     arcpy.Union_analysis(in_features=['2011.gdb\\clu_2011_fp_r','2011.gdb\\clu_2011_fp'], out_feature_class='clu_2011_fp_r_c')
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))



            # elif year not in [2003,2010]:
            elif year not in [2011]:
                fc_prev = "{0}.gdb\\clu_{0}_fp_r".format(str(year-1))
                arcpy.Erase_analysis(in_features=fc_prev,erase_features='{0}.gdb\\clu_{0}_crop'.format(str(year)), out_feature_class='{0}.gdb\\clu_{0}_fp_r'.format(str(year)))
                arcpy.Union_analysis(in_features=['{0}.gdb\\clu_{0}_fp_r'.format(str(year)),'{0}.gdb\\clu_{0}_fp'.format(str(year))], out_feature_class='clu_{}_fp_r_c'.format(str(year)))



def create_Crop_byYear():
    ## Description: go through each [year].gdb and select only features that fit the crop condition (i.e. 'CLUCLSCD=2' OR 'CROPLND3CM=1') store this as clu_[year]_crop.
    ## Add this clu_[year]_crop to the pervious years cropped dataset to create the cumulative dataset.

    yearlist = range(2011,2016)

    ##create an array to hold all files from all state gdbs for a given year
    for year in yearlist:
        print 'current year:', year
        
        ##define current working geodatabase
        gdb = "G:\\ancillary_storage\\intactland\\intact_clu\\main\\years\\{}.gdb".format(str(year))
        
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
            os.chdir('G:\\ancillary_storage\\intactland\\intact_clu\\main\\years')
            
            ##----create the yearly cummalative crop dataset-------------------------------------------------------
            # if year == 2004:
            #     fc_prev = "D:\\projects\\intact_land\\years\\2003.gdb\\clu_2003_crop"
            #     arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_2004_crop_c')
            #     ##----repair geometry
            #     # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))
            if year == 2012:
                fc_prev = "2011.gdb\\clu_2011_crop"
                arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_2012_crop_c')

            # elif year == 2011:
            #     fc_2009 = "D:\\projects\\intact_land\\years\\2009.gdb\\clu_2009_crop_c"
            #     arcpy.Union_analysis(in_features=[fc_2009,layer], out_feature_class='clu_2011_crop_c')
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))
            # elif year>2004 & year!=2010 & year!=2011:
            elif year not in [2011]:
                fc_prev = "{0}.gdb\\clu_{0}_crop_c".format(str(year-1))
                arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_{}_crop_c'.format(str(year)))
                ##----repair geometry
                # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))




def create_nonFP_byYear():
    ##Description: merge all the couties for a given year and then use erase() function with clu_[year]_crop to get non crop dataset


    yearlist = range(2005,2016)
    
    ##create an array to hold all files from all state gdbs for a given year
    for year in yearlist:
        print year
        countylist= []

        arcpy.env.workspace = "D:\\projects\\intactland\\intact_clu\\main\\states"

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
            arcpy.env.workspace = "G:\\ancillary_storage\\intactland\\intact_clu\\main\\years"
            in_features = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties'
            layer = 'counties_{}'.format(str(year))
            where_clause = createWhereString(countylist)
            
            # # # Make a layer from the feature class
            arcpy.MakeFeatureLayer_management(in_features, layer, where_clause)
            
            #create a feature class containing a counties with data for a given year
            # arcpy.FeatureClassToFeatureClass_conversion(layer, "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year)), "clu_{}_counties".format(str(year)))
            
            ##------------create to noncrop dataset per year ---------------------------------------------------------------------------
            arcpy.Erase_analysis(in_features=layer, erase_features="{0}.gdb\\fp_{0}".format(str(year)), out_feature_class="{0}.gdb\\nfp_{0}".format(str(year)))
           

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
            arcpy.FeatureClassToFeatureClass_conversion(layer, "D:\\projects\\intact_land\\years\\{}.gdb".format(str(year)), 'clu_{}_counties_c'.format(str(year)))
            
            

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




def convertPoly2Raster():
    yearlist=range(2012,2016)
    # yearlist=[2003]
    print yearlist
    yearlist = [e for e in yearlist if e != 2010]
    print yearlist

    reclassed_rasters = []

    for year in yearlist:
        print(year)
        ###format year to get single digit
        # year=str(year)[1:].replace("0", "")
        arcpy.env.workspace = 'D:\\projects\\intactland\\intact_clu\\main\\years\\{}.gdb'.format(year)

        in_features = 'clu_{}_crop_c'.format(year)
        dissolved_fc = '{}_d'.format(in_features)
        dissolved_raster = '{}_raster'.format(dissolved_fc)
        dissolved_raster_zero = '{}_zero'.format(dissolved_raster)
        histo = '{}_histo'.format(in_features)



        print dissolved_fc

        # arcpy.Dissolve_management(in_features, out_feature_class=dissolved_fc, dissolve_field="CLUCLSCD")

        # ####add field to fc and populate field with year
        # arcpy.AddField_management(dissolved_fc, "year", "SHORT")
        # gen.addValue2Field(fc=dissolved_fc, field='year', value=int(str(year)[1:].replace("0", "")))
        
        # #####covert dissolved featureclass to a raster
        # cdl_2015='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2015'
        # arcpy.env.snapRaster = cdl_2015
        # arcpy.env.cellsize = cdl_2015
        # arcpy.env.extent = cdl_2015
        # arcpy.PolygonToRaster_conversion(dissolved_fc, 'year', dissolved_raster, 'CELL_CENTER', 'year', 30)

        #####create zonal histogram for raster
        # ZonalHistogram (in_zone_data='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties', zone_field='ATLAS_STCO', in_value_raster=dissolved_raster, out_table=histo)


        #### use zonal_histogram to get the counts of unique trajectories by county
        gen.addGDBTable2postgres_histo_county(pgdb='intact_lands', schema='clu_{}'.format(str(year)), currentobject=histo)



        ####reclass the rasters to convert null to zero
        # reclassed_rasters.append(Con(IsNull(dissolved_raster),0,dissolved_raster))
        # reclassed_raster=Con(IsNull(dissolved_raster),0,dissolved_raster)
        # reclassed_raster.save(dissolved_raster_zero)






    ####combine all rasters to create a dataset that represents years of conversion
    # gen.addGDBTable2postgres_raster(gdb='D:\\projects\\intactland\\intact_clu\\final\\intactlands_new.gdb', pgdb='intact_lands', schema='intact_clu', table='intact_clu_ytc')

    #### use zonal_histogram to get the counts of unique trajectories by county
    # gen.addGDBTable2postgres_histo_county(pgdb='intact_lands', schema='intact_clu', currentobject='D:\\projects\\intactland\\intact_clu\\final\\intactlands_new.gdb\\intact_clu_ytc_zonal_cnty')




def createCumulaitve(root_dir, years, dataset):

    # Set the workspace environment to local file geodatabase
    os.chdir(root_dir)

    for year in years:

        if year == 2004:
            fc_prev = "2003.gdb\\clu_2003"
            arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_2004_{}')
            ##----repair geometry
            # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))

        elif year == 2011:
            fc_2009 = "2009.gdb\\clu_2009_{}"
            arcpy.Union_analysis(in_features=[fc_2009,layer], out_feature_class='clu_2011_crop_c')
            ##----repair geometry
            # arcpy.RepairGeometry_management('clu_{}_crop_cumm'.format(str(year)))
        elif year > 2004 & year != 2011:
            fc_prev = "{0}.gdb\\clu_{0}_crop_c".format(str(year-1))
            arcpy.Union_analysis(in_features=[fc_prev,layer], out_feature_class='clu_{}_crop_c'.format(str(year)))





######################  call functions  ###############################################################
######--- creation of CLU by year code  ------#########################
# create_fp_ByYear()

# create_nonFP_byYear()

######--- crop code  ------############################################
# create_Crop_byYear()
# convertPoly2Raster()

########  footprint ######################################
# create_fp_byYear()

######--- noncrop -----###############################################
# create_NonCrop_byYear()
# create_NonCrop_byYear_cumulative()



###### sandbox#################\

# year_list=range(2005,2008)
# year_list=[2004]

# for year in year_list:
#     print year
#     gen.convertFCtoPG(gdb='D:\\projects\\intactland\\intact_clu\\main\\years\\{}.gdb'.format(year), pgdb='intact_lands', schema='clu_{}'.format(year), table='clu_{}_crop_c_d'.format(year), epsg=102003)


# gen.convertFCtoPG(gdb='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb', pgdb='intact_lands', schema='spatial', table='counties_102003', epsg=102003)


# ### describe data  ###################
# # gdbToPG('noncrop_c')
# #unionTables()

# import arcpy
# from arcpy import env
# from arcpy.sa import *

# # Check out the ArcGIS Spatial Analyst extension license
# arcpy.CheckOutExtension("Spatial")


# env.workspace = "D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\compare.gdb"
# in_raster1 = Raster('swift_clu_2015_noncrop_c_w_masks_raster_zero')
# in_raster2 = Raster('swift_nlcd_intact_modified_zero')
# outraster = 'D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\compare.gdb\\swift_clu_2015_noncrop_c_w_masks_raster_zero'

# # outCon2 = Con(IsNull(in_raster), 0, 1)
# # outCon2.save(outraster)


# outCombine = Combine([in_raster1, in_raster2])
# outCombine.save("comb")







































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






























