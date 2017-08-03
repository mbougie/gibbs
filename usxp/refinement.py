##### Import system modules
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2

'''
Description---
This script is meant to refine the intial trajectory by removing false change from each landcover defined.
'''

# set the engine for psndas
engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

# set con for psycopg2
try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### import extension
arcpy.CheckOutExtension("Spatial")

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


def getQuanitiativeFocusCounties():

    def createEmptyTable(table_list):
        for table in table_list:
            cur = conn.cursor()
            query="CREATE TABLE refinement.counties_yfc_"+table+"_temp(stco text, lc text, acres numeric)"
            print query
            cur.execute(query)
            conn.commit()


    def transposeTable(gdb_path, wc):

        arcpy.env.workspace = defineGDBpath(gdb_path)

        for table in arcpy.ListTables(wc): 
            print 'table: ', table
            fields = arcpy.ListFields(table)
            
            for field in fields:
                #constrant column names by excluding the below fields from the processing
                if field.name == 'OBJECTID' or field.name == 'ATLAS_STCO':
                    print field.name
                else:
                    # loop through each row and get the value for specified columns
                    rows = arcpy.SearchCursor(table)
                    for row in rows:
                        lc = row.getValue(field.name)
                        stco = row.getValue('ATLAS_STCO')
                        print 'table: ', table
                        print 'stco: ', stco
                        print 'field.name: ', field.name
                        print 'lc: ', lc
                        
                        cur = conn.cursor()
                        query="INSERT INTO refinement."+wc+"_temp VALUES ('" + str(stco) + "' , '" + str(field.name) + "' , " + str(lc) + ")"
                        print query
                        cur.execute(query)
                        conn.commit()
    

    def createTableAS(table_list):
        for table in table_list:
            cur = conn.cursor()
            query="create table refinement.counties_yfc_"+table+" as SELECT a.*, round(a.acres/b.total_acres * 100,2) as percent FROM refinement.counties_yfc_"+table+"_temp as a, (SELECT stco, sum(acres) total_acres FROM refinement.counties_yfc_"+table+"_temp group by stco) as b where a.stco = b.stco and acres <> 0 order by stco, percent desc"
            print query
            cur.execute(query)
            conn.commit()


    def dropTable(table_list):
        for table in table_list:
            cur = conn.cursor()
            query="DROP TABLE refinement.counties_yfc_"+table+"_temp"
            print query
            cur.execute(query)
            conn.commit()


    def createReferenceTable():
        cur = conn.cursor()
        query="CREATE TABLE refinement.focus_counties_yfc2 as SELECT counties_yfc_bfnc.stco, counties_yfc_bfnc.lc as crop, counties_yfc_bfnc.max_bfnc_percent, counties_yfc_fnc.lc as noncrop, counties_yfc_fnc.max_fnc_percent, counties_yfc_years.lc year_to_nc, counties_yfc_years.max_years_percent FROM ( select a.*, b.max_bfnc_percent from refinement.counties_yfc_bfnc as a,  (SELECT stco, max(percent) as max_bfnc_percent FROM refinement.counties_yfc_bfnc group by stco) as b where a.stco = b.stco and a.percent = b.max_bfnc_percent) as counties_yfc_bfnc,( select a.*, b.max_fnc_percent from refinement.counties_yfc_fnc as a,  (SELECT stco, max(percent) as max_fnc_percent FROM refinement.counties_yfc_fnc group by stco) as b where a.stco = b.stco and a.percent = b.max_fnc_percent) as counties_yfc_fnc,( select a.*, b.max_years_percent from refinement.counties_yfc_years as a,(SELECT stco, max(percent) as max_years_percent FROM refinement.counties_yfc_years group by stco) as b where a.stco = b.stco and a.percent = b.max_years_percent) as counties_yfc_years WHERE counties_yfc_bfnc.stco = counties_yfc_fnc.stco AND counties_yfc_fnc.stco = counties_yfc_years.stco order by counties_yfc_bfnc.max_bfnc_percent"
        print query
        cur.execute(query)
        conn.commit()
    
      
    ###### call functions #####################
    table_list = ['bfnc','fnc','years']

    createEmptyTable(table_list)
    gen.transposeTable(['refinement','refinement'],'counties_yfc_years')
    CreateTableAS(table_list)
    dropTable(table_list)
    createReferenceTable()

def createKMLfile():
    # Set environment settings
    

    def rasterToPoly():
        arcpy.env.workspace = defineGDBpath(['post','yfc'])
        for raster in arcpy.ListDatasets('*_fnl', "Raster"): 
            print 'raster:', raster

            # Set local variables
            in_raster = raster
            out_polygon_features = defineGDBpath(['refinement','refinement']) + raster + '_shp'
            simplify = "NO_SIMPLIFY"
            raster_field = "VALUE"

            # Execute RasterToPolygon
            arcpy.RasterToPolygon_conversion(in_raster, out_polygon_features, simplify, raster_field)

    def stackMutipleFC():
        arcpy.env.workspace = defineGDBpath(['refinement','refinement'])

        # # Set local variables
        in_features = arcpy.ListFeatureClasses("*_shp")
        print in_features
        out_feature_class = "stacked_features"
        join_attributes = "NO_FID"
        cluster_tolerance = 0.0003
        arcpy.Union_analysis (in_features, out_feature_class, join_attributes, cluster_tolerance)

    def clipFCtoCounty():
        arcpy.env.workspace = defineGDBpath(['refinement','refinement'])
        # Use the ListFeatureClasses function to return a list of shapefiles.
        fc = 'focus_counties'

        cursor = arcpy.da.SearchCursor(fc, ['atlas_stco'])
        for row in cursor:
            print(row[0])
            
            layer = "layer_" + row[0]
            where_clause = "atlas_stco = '" + row[0] + "'"

     


            # Set local variables
            in_features = 'stacked_features'
            clip_features = arcpy.MakeFeatureLayer_management(fc,layer, where_clause)
            out_feature_class = "stco_"+row[0]
            xy_tolerance = ""

            # Execute Clip
            arcpy.Clip_analysis(in_features, clip_features, out_feature_class, xy_tolerance)

    def featureToKML():
        arcpy.env.workspace = defineGDBpath(['refinement','refinement'])
        # Use the ListFeatureClasses function to return a list of shapefiles.
        filename = "stco_*"
        featureclasses = arcpy.ListFeatureClasses(filename)

        # Copy shapefiles to a file geodatabase
        for fc in featureclasses:
            print fc

            # create directories to hold kml file and associated images
            stco_dir = rootpath + 'refinement/yfc/' + fc + '/'
            if not os.path.exists(stco_dir):
                os.makedirs(stco_dir)
            # Set local variables
            # Make a layer from the feature class
            arcpy.MakeFeatureLayer_management(fc,fc)

            out_kmz_file =  stco_dir + fc + '.kmz'
            arcpy.LayerToKML_conversion (fc, out_kmz_file)
    
    ###### call functions #####################
    # rasterToPoly()
    # stackMutipleFC()
    # clipFCtoCounty()
    featureToKML()

def falseConversion():


    def reclassifyRaster():
        # Description: reclass cdl rasters based on the specific arc_reclassify_table 

        # Set environment settings
        arcpy.env.workspace = defineGDBpath(['ancillary','cdl'])

        for raster in arcpy.ListDatasets('*', "Raster"): 
            print 'raster:', raster

            outraster = raster.replace("_", "_r_")

            print outraster 

            #define the output
            output = defineGDBpath(['pre','reclass'])+outraster
            print 'output: ', output

            return_string=getReclassifyValuesString()

            # Execute Reclassify
            arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")


    def getReclassifyValuesString():
        #Note: this is a aux function that the reclassifyRaster() function references
        cur = conn.cursor()

        #DDL: add column to hold arrays
        cur.execute('SELECT value::text,test FROM misc.lookup_cdl WHERE test IS NOT NULL ORDER BY value');
        
        #create empty list
        reclassifylist=[]

        # fetch all rows from table
        rows = cur.fetchall()
        
        # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
        for row in rows:
            ww = [row[0] + ' ' + row[1]]
            reclassifylist.append(ww)

        #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
        columnList = ';'.join(sum(reclassifylist, []))
        print columnList
        
        #return list to reclassifyRaster() fct
        return columnList


    def createTrajectories(wc):

        # Set environment settings
        arcpy.env.workspace = defineGDBpath(['pre','reclass'])
        
        #get a lsit of all rasters in sepcified database
        rasterList = arcpy.ListDatasets('cdl_'+wc+'*', "Raster")
        
        #sort the rasterlist by accending years
        rasterList.sort(reverse=False)
        
        #prepend nlcd raster name 
        rasterList.insert(0, 'nlcd_b_2011')
        print 'rasterList: ',rasterList

        #Execute Combine
        outCombine = Combine(rasterList)
        print 'outCombine: ', outCombine
        
        output = defineGDBpath(['pre','trajectories'])+'traj_'+wc
        
        #Save the output 
        outCombine.save(output)


    def addGDBTable2postgres(gdb_args,tablename,pg_shema):


        # path to the table you want to import into postgres
        input = defineGDBpath(gdb_args)+tablename

        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(input)]
        
        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(input,fields)
        print arr
        
        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df
        
        # use pandas method to import table into psotgres
        df.to_sql(tablename, engine, schema=pg_shema)


    def PG_DDLandDML(degree_lc):

        #define cursor
        cur = conn.cursor()
        
        # add column to table to hold arrays
        cur.execute('ALTER TABLE pre.traj_' + degree_lc + ' ADD COLUMN traj_array integer[];');
        
        # insert values into array column
        cur.execute('UPDATE pre.traj_' + degree_lc + ' SET traj_array = ARRAY[nlcd_b_2011,cdl_' + degree_lc + '_2010,cdl_' + degree_lc + '_2011,cdl_' + degree_lc + '_2012,cdl_' + degree_lc + '_2013,cdl_' + degree_lc + '_2014,cdl_' + degree_lc + '_2015,cdl_' + degree_lc + '_2016];');
        
        #commit the changes
        conn.commit()
        print "Records created successfully";

        #close..........
        conn.close()


    def createTrajMask(degree_lc):
        arcpy.env.workspace = defineGDBpath(['pre','trajectories'])

        df = pd.read_sql_query("select a.\"Value\",b.mtr from pre.traj_"+degree_lc+" as a JOIN pre.traj_r_lookup as b ON a.traj_array = b.traj_array",con=engine)
        
        print df
        a = df.values
        print a
        print type(a)

        l=a.tolist()
        print type(l)
        print l

        for raster in arcpy.ListDatasets('*'+degree_lc, "Raster"): 
            print 'in raster: ', raster
            output = raster+'_msk'
            print 'output raster: ', output
            outReclass = Reclassify(raster, "Value", RemapRange(l), "NODATA")
            
            outReclass.save(output)



    def mosaicRasters():

        ##STILL NEED TO DEVLOP
        arcpy.env.workspace = defineGDBpath(['pre','trajectories'])

        # Execute Con
        outCon = Con(IsNull('traj_r_msk'), 'traj_b', 'traj_r_msk')

        outCon.save("traj")
    
    ######  call functions  #############################
    reclassifyRaster()
    createTrajectories("r")
    addGDBTable2postgres(['pre','trajectories'],'traj_r','pre')
    PG_DDLandDML('r')
    createTrajMask('r')
    mosaicRasters()








##########################################################
######  call main functions  #############################
##########################################################

# getQuanitiativeFocusCounties()
createKMLfile()
# falseConversion()


