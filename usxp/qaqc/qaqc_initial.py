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

import general as gen



# conv_coef=0.000247105
# '''CONSTANTS'''
# 0.774922476

coef={'acres':1,'msq':0.000247105,'30m':0.222395,'56m':0.774922476}




'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"




rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



def getRasterCount(gdb_path, wc):
    arcpy.env.workspace = defineGDBpath(gdb_path)

    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',raster

        list_count=[]
        list_acres=[]


        
        #loop through each row and get the value for specified columns
        rows = arcpy.SearchCursor(raster)
        for row in rows:
        
            count = row.getValue('Count')
            print count
            print type(count)
            list_count.append(count)
            
            res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")

            #convert the result object into and integer
            res = res.getOutput(0)
            print res
            print type(res)
            acreage = g.getAcres(int(count), int(res))
            list_acres.append(acreage)
            
           

        query3="DELETE FROM qaqc.counts_rasters WHERE dataset = '" + raster + "'"
        g.commitPG(query3)

        cur = conn.cursor()
        query="INSERT INTO qaqc.counts_rasters VALUES ('" + str(raster) + "' , " + str(sum(list_count)) + " , " + str(res) + " , " + str(sum(list_acres))+ ")"
        print query
        cur.execute(query)
        conn.commit()


    #call updateLookupTable() function to update table
    updateLookupTable()






def countDiff():
    query=  """  SELECT a.dataset,
                    b.acres AS acres_child,
                    a.parent,
                    c.acres AS acres_parent,
                    c.acres - b.acres AS diff_acres,
                    (1::double precision - b.acres / c.acres) * 100::double precision AS diff_percent
                   FROM qaqc.lookup_inheritance a,
                    qaqc.counts_tables b,
                    qaqc.counts_rasters c
                  WHERE a.dataset = b.dataset AND a.parent = c.dataset AND a.process = true
                UNION
                 SELECT a.dataset,
                    b.acres AS acres_child,
                    a.parent,
                    c.acres AS acres_parent,
                    c.acres - b.acres AS diff_acres,
                    (1::double precision - b.acres / c.acres) * 100::double precision AS diff_percent
                   FROM qaqc.lookup_inheritance a,
                    qaqc.counts_rasters b,
                    qaqc.counts_rasters c
                  WHERE a.dataset = b.dataset AND a.parent = c.dataset AND a.process = true;"""
    print query
    rows=g.fetchPG(query)
    for row in rows:
        print row
        cur = conn.cursor()
        query = "UPDATE qaqc.counts_diff SET (dataset, acres, parent, acres_parent, diff_acres, diff_perc) = "+str(row)+" WHERE dataset = '"+ str(row[0]) + "'"
        
        print query
        cur.execute(query)
        conn.commit()









def getTableCount(dataset):

    schema = "'deliverables'"
    # dataset = "'gsconv_%_lcc_counties'"
    tables = g.getPGTablesList(schema,dataset)
    print tables

    for table in tables:
        table_w_qoutes= "'"+table+"'"
        # wc="'%value%'"
        columnlist=g.getPGColumnsList(schema, table_w_qoutes)

        query1='SELECT sum('+columnlist+') FROM deliverables.'+table
        print query1
        
        query2="SELECT units FROM qaqc.lookup_inheritance where dataset = "+table_w_qoutes
        print query2
        count = g.fetchPG(query1)
        print count
        unit = g.fetchPG(query2)
        print unit
        coefficient=coef.get(unit[0][0])
        print coefficient
        
        query3="DELETE FROM qaqc.counts_tables WHERE dataset = " + table_w_qoutes
        print query3
        g.commitPG(query3)

        query4="INSERT INTO qaqc.counts_tables VALUES (" + table_w_qoutes + " , " + str(count[0][0]*coefficient)+ ")"
        print query4
        g.commitPG(query4)
        
       


def getDerivedTableCounts(parent):
    out_table="'"+parent+'_lcc'+"'"
    query1= """ SELECT 
                    a.resolution,
                    sum(a.acres)
                FROM 
                    qaqc.counts_rasters as a,
                    qaqc.lookup_inheritance as b

                WHERE 
                    a.dataset = b.dataset AND
                    b.parent = '""" + parent + """'
                GROUP BY a.resolution
            """
    print query1
    result = g.fetchPG(query1)

    query3="INSERT INTO qaqc.counts_tables VALUES (" + out_table +" , " + str(result[0][1]) + ")"
    g.commitPG(query3)

  




def updateLookupTable():
    #used as a function inside getRasterCount() function
    query = """
            INSERT INTO qaqc.lookup_inheritance (dataset)(
            SELECT b.dataset
            FROM qaqc.lookup_inheritance as a RIGHT OUTER JOIN
            (SELECT 
            counts_rasters.dataset
            FROM 
            qaqc.counts_rasters
            UNION

            SELECT
            counts_tables.dataset
            FROM
            qaqc.counts_tables) as b

            on a.dataset = b.dataset

            WHERE a.dataset is null 
            );
            """
    g.commitPG(query)

# def addFIeldtoRaster(gdb_path, wc):
#     arcpy.env.workspace = defineGDBpath(gdb_path)
    
#     fieldnames=['acres']
    
#     for attributetable in arcpy.ListDatasets(wc, "Raster"): 
#         print 'raster: ',attributetable
#         for field in fieldnames:
#             in_table = attributetable
#             field_name = field
#             field_type = "DOUBLE"
#             # fieldName1 = "acres"

         
#             # # Execute AddField twice for two new fields
#             # arcpy.AddField_management(in_table=attributetable, field_name=field, field_type="DOUBLE")
  

#             fieldCalculator(attributetable, field)


# def fieldCalculator(attributetable, field):

#     #convert the result object into and integer
#     res = arcpy.GetRasterProperties_management(attributetable, "CELLSIZEX")

#     #convert the result object into and integer
#     res = res.getOutput(0)
#     print res
#     print type(res)
#     coefficient=coef.get(int(res))

#     # acreage = g.getAcres(int(count), int(res))
#     expression = "getCoef(!Count!,int(res))"

#     codeblock = """def getCoef(count,res):
#         return 3
    
#     """

#     # expression = "getClass(!Count!,int(res))"
#     # codeblock = """g.getAcres"""

     
     
#     # Execute CalculateField 
#     arcpy.CalculateField_management(attributetable, field, expression, "PYTHON_9.3", codeblock)


# getlookup(shema,dataset)











##########  call addGDBTable2postgres  #########################################
# g.addGDBTable2postgres(['deliverables','deliverables_refined'],'mtr_counties','deliverables')

######  call getRasterCount() function  ##############################
# getRasterCount(['deliverables','deliverables_refined'],'*')
# getRasterCount(['deliverables','xp_update_refined'],'*')
# getRasterCount(['ancillary','cdl'],'*cdl_2012*')
# getRasterCount(['ancillary','xp_initial'],'*')
# getRasterCount(['post','yfc_test'],"*fnl")


######  call getTableCounts() function  ##############################
# getTableCount("'%'")
# getTableCount("'gsconv_%_lcc_counties'")




######  call getDerivedTableCounts() function  ##############################
# getDerivedTableCounts("gsconv_new")


##########  call countDiff() function  #######################################
# countDiff("'gsConv_old'", "'class_before_crop'", "'subset to grassland and shrubland AND class0 is included in count'")
# countDiff("'gsConv_new'", "'bfc'", "'subset to grassland and shrubland'")
# countDiff("'gsConv_new_lcc'", "'gsConv_new'", "'lcc is null maybe'")
# countDiff("'gsConv_old_lcc'","'gsConv_old'", "'lcc is null maybe'")



# countDiff()



# g.addRasterAttributeTable(['deliverables','xp_update_refined'],'mtr')




# addFIeldtoRaster(['post','yfc_test'],"*fnc_fnl")



class CoreObject:

    def __init__(self, wc):
        # self.years = years

        # if self.years[1] == 2016:
        #     self.datarange = str(self.years[0])+'to'+str(self.years[1]-1)
        #     print 'self.datarange:', self.datarange
            
        # else:

        #     self.datarange = str(self.years[0])+'to'+str(self.years[1])
        #     print 'self.datarange:', self.datarange


        # # self.traj_name = "traj_cdl"+self.res+"_b_"+self.datarange+"_rfnd"
        # # self.traj_path = defineGDBpath(['pre','trajectories'])+self.traj_name
        # # self.filter = filter
        # # self.wc = "*"+res+"*"+self.datarange+"*"
        # self.mmu = mmu
        self.wc = wc








gdb_args = ['post','ytc']



qaqc = CoreObject(
      #wc
      '*56_2008to2015_*nbl'
      )







def addGDBTable2postgres():
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)
    
    # wc = '*'+qaqc.res+'*'+qaqc.datarange+'*'+qaqc.filter+'*_clean'+qaqc.mmu+'_nbl'
    # wc = 'mtr'
    # print wc


    for raster in arcpy.ListDatasets(qaqc.wc, "Raster"): 

    # for table in arcpy.ListTables(wc): 
        print 'raster: ', raster
        
        # res = Raster(raster).spatialReference
        res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")
        #Get the elevation standard deviation value from geoprocessing result object
        # elevSTD = res.getOutput(0)
        # # print elevSTD
        
        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(raster)]
        print fields

        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(raster,fields)
        print arr

        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        # use pandas method to import table into psotgres
        df.to_sql(raster, engine, schema='counts')

        # add trajectory field to table
        addAcresField(raster, 'counts', str(res.getOutput(0)))





def addAcresField(tablename, schema, res):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN acres bigint;');
    
    #DML: insert values into new array column
    cur.execute('UPDATE '+ schema + '.' + tablename + ' SET acres = count * ' + gen.getPixelConversion2Acres(res));
    
    conn.commit()
    print "Records created successfully";





def clipByMMUmask():
    #define workspace
    # arcpy.env.workspace=defineGDBpath(['core', 'mmu'])

    raster = defineGDBpath(['ancillary','temp'])+'rg_mtr_v1'
    print 'raster: ', raster

    # for count in masks_list:
    cond = "Count > 20"
    print 'cond: ',cond

    output = defineGDBpath(['ancillary','temp'])+'rg_mtr_v1_belowmmu'

    print output

    outSetNull = SetNull(raster, 1, cond)

    # Save the output 
    outSetNull.save(output)

    gen.buildPyramids(output)





def getIntersectionofRasters():

    raster1 = Raster(defineGDBpath(['ancillary','data_2008_2012'])+'Multitemporal_Results_FF2')
    raster2 = Raster(defineGDBpath(['ancillary','temp'])+'rg_mtr_v1_belowmmu')
    output = defineGDBpath(['ancillary','temp'])+'rg_mtr_v1_belowmmu_mtr3'


    outCon = Con(((raster1 == 3) & (raster2 == 1)), 100)

    outCon.save(output)

    gen.buildPyramids(output)









def addGDBTable2postgres():


    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(['refine','trajectories'])
    
    # wc = '*'+core.res+'*'+core.datarange+'*'+core.filter+'*_msk5_nbl'
    wc = 'traj_yfc30_2008to2016'
    print wc


    for raster in arcpy.ListDatasets(wc, "Raster"): 

    # for table in arcpy.ListTables(wc): 
        print 'raster: ', raster
        
        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(raster)]
        print fields

        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(raster,fields)
        print arr

        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        # use pandas method to import table into psotgres
        df.to_sql(raster, engine, schema='refinement')

        #add trajectory field to table
        addAcresField(raster, 'refinement')

        #add trajectory field to table
        addTrajArrayField(raster, fields, 'refinement')





def addAcresField(tablename, schema):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN acres bigint;');
    
    #DML: insert values into new array column
    cur.execute('UPDATE '+ schema + '.' + tablename + ' SET acres = count * ' + gen.getPixelConversion2Acres('30'));
    
    conn.commit()
    print "Records created successfully";
    # conn.close()



def addTrajArrayField(tablename, fields, schema):
    #this is a sub function for addGDBTable2postgres()

    cur = conn.cursor()

    #convert the rasterList into a string
    columnList = ','.join(fields[3:])
    print columnList

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN traj_array integer[];');

    #DML: insert values into new array column
    cur.execute('UPDATE ' + schema + '.' + tablename + ' SET traj_array = ARRAY['+columnList+'];');

    conn.commit()
    print "Records created successfully";
    # conn.close()





#################  CODE FOR THE COVERSION BY YEAR ##############################################################

# STEP1--------------------------------------
# create table refinement.core as
# SELECT b.value,a.yfc30_2008to2011 as bfnc, a.yfc30_2008to2012 as fnc, a.traj_array, c.serial, b.yfc30_2008to2016 as year, b.acres FROM refinement.traj_yfc30_2008to2016 as a, refinement.traj_yfc30_2008to2016_byyears as b,
# refinement.traj_yfc30_2008to2016_lookup as c
# WHERE b.traj_yfc30_2008t = a.value 
# and a.traj_array = c.traj_array
# order by year,acres desc



# STEP2-----------------------------------------
# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
# query1 = 'SELECT * FROM refinement.core'
# df = pd.read_sql_query(query1, con=engine)
# print df

# # # print df[df.traj_array == "[61, 176]"]

# df = df.pivot(index='value', columns='year', values='acres')

# table = 'core_pivot'
# df.to_sql(table, engine, schema='refinement')




# query2 = 'SELECT a."2010", a."2011", a."2012", a."2013", d.serial FROM refinement.try_this as a, refinement.traj_yfc30_2008to2016_byyears as b, refinement.traj_yfc30_2008to2016 as c, refinement.traj_yfc30_2008to2016_lookup as d WHERE a.value = b.value AND b.traj_yfc30_2008t = c.value and c.traj_array = d.traj_array'
# df = pd.read_sql_query(query2, con=engine)
# # print df
# # df1 = df.apply(lambda x: pd.Series(x.dropna().values))
# # print df1
# print df.groupby(level=0).sum()
# grouped = df.groupby(['2010', 'serial'])
# result = grouped.agg(combine_it)
# df_2010 = df.groupby(['2010', 'serial']).first().reset_index()
# df_2011 = df.groupby(['2011', 'serial']).first().reset_index()
# df_2012 = df.groupby(['2012', 'serial']).first().reset_index()
# df_2013 = df.groupby(['2013', 'serial']).first().reset_index()
# print df_2010
# print df_2011


 
# print df_2010.merge(df_2011, left_on='serial', right_on='serial', how='outer')

##call functions
# addGDBTable2postgres()
# clipByMMUmask()
# getIntersectionofRasters()
# addGDBTable2postgres()





# SELECT b.value,a.yfc30_2008to2011 as bfnc, a.yfc30_2008to2012 as fnc, a.traj_array, c.serial, b.yfc30_2008to2016 as year, b.acres FROM refinement.traj_yfc30_2008to2016 as a, refinement.traj_yfc30_2008to2016_byyears as b,
# refinement.traj_yfc30_2008to2016_lookup as c
# WHERE b.traj_yfc30_2008t = a.value 
# and a.traj_array = c.traj_array
# and b.acres > 100000 order by year,acres desc



# SELECT a."2010", a."2011", a."2012", a."2013", d.serial FROM refinement.try_this as a, refinement.traj_yfc30_2008to2016_byyears as b, refinement.traj_yfc30_2008to2016 as c,
# refinement.traj_yfc30_2008to2016_lookup as d
 
# WHERE a.value = b.value 
# AND b.traj_yfc30_2008t = c.value
# and c.traj_array = d.traj_array



# Final step-------------------------------------------------
# create table refinement.traj_yfc30_2008to2016_conversion_series as

# SELECT 

#   round((cast(conversion_series_step1."2010" as numeric) * 0.222395),0) as cy_2010, 
#   round((cast(conversion_series_step1."2011" as numeric) * 0.222395),0) as cy_2011, 
#   round((cast(conversion_series_step1."2012" as numeric) * 0.222395),0) as cy_2012, 
#   round((cast(conversion_series_step1."2013" as numeric) * 0.222395),0) as cy_2013,  
#   round((cast(conversion_series_step1."2014" as numeric) * 0.222395),0) as cy_2014, 
#   round((cast(conversion_series_step1."2015" as numeric) * 0.222395),0) as cy_2015, 
#   traj_yfc30_2008to2016_lookup.traj_array
# FROM 
#   refinement.conversion_series_step1, 
#   refinement.traj_yfc30_2008to2016_lookup
# WHERE 
#   traj_yfc30_2008to2016_lookup.serial = conversion_series_step1.serial

# order by "2010" desc





# ALL SQL TO DIG THROUGH--------------------------------------####################____________________________--------------------------------------------------------
# create table refinement.core2 as
# SELECT b.value,a.yfc30_2008to2011 as bfnc, a.yfc30_2008to2012 as fnc, a.traj_array, c.serial, b.yfc30_2008to2016 as year, b.acres FROM refinement.traj_yfc30_2008to2016 as a, refinement.traj_yfc30_2008to2016_byyears as b,
# refinement.traj_yfc30_2008to2016_lookup as c
# WHERE b.traj_yfc30_2008t = a.value 
# and a.traj_array = c.traj_array
# order by year,acres desc


# create table refinement.core_pivot_serial as
# SELECT d.serial, a."2010", a."2011", a."2012", a."2013", a."2014", a."2015" 
# FROM refinement.core_pivot as a, refinement.traj_yfc30_2008to2016_byyears as b, refinement.traj_yfc30_2008to2016 as c,
# refinement.traj_yfc30_2008to2016_lookup as d
 
# WHERE a.value = b.value 
# AND b.traj_yfc30_2008t = c.value
# and c.traj_array = d.traj_array;



# create table refinement.conversion_series_step1 as 
# WITH CTE AS (
#     SELECT
        
#         MAX("2010") AS "2010",
#         MAX("2011") AS "2011",
#         MAX("2012") AS "2012",
#         MAX("2013") AS "2013",
#         MAX("2014") AS "2014",
#         MAX("2015") AS "2015",
#         serial
#     FROM refinement.core_pivot_serial
#     GROUP BY serial
#     HAVING MAX("2010") = MIN("2010")
#         AND MAX("2011") = MIN("2011")
#         AND MAX("2012") = MIN("2012")
#         AND MAX("2013") = MIN("2013")
#         AND MAX("2014") = MIN("2014")
#         AND MAX("2015") = MIN("2015")
# )
#     SELECT *
#     FROM CTE
#     UNION ALL
#     SELECT *
#     FROM refinement.core_pivot_serial
#     WHERE serial NOT IN (SELECT serial FROM CTE)

#     order by "2010" desc






# create table refinement.traj_yfc30_2008to2016_conversion_series as

# SELECT 

#   round((cast(conversion_series_step1."2010" as numeric) * 0.222395),0) as cy_2010, 
#   round((cast(conversion_series_step1."2011" as numeric) * 0.222395),0) as cy_2011, 
#   round((cast(conversion_series_step1."2012" as numeric) * 0.222395),0) as cy_2012, 
#   round((cast(conversion_series_step1."2013" as numeric) * 0.222395),0) as cy_2013,  
#   round((cast(conversion_series_step1."2014" as numeric) * 0.222395),0) as cy_2014, 
#   round((cast(conversion_series_step1."2015" as numeric) * 0.222395),0) as cy_2015, 
#   traj_yfc30_2008to2016_lookup.traj_array
# FROM 
#   refinement.conversion_series_step1, 
#   refinement.traj_yfc30_2008to2016_lookup
# WHERE 
#   traj_yfc30_2008to2016_lookup.serial = conversion_series_step1.serial

# order by "2010" desc



# def createKMLfile():
#     # Set environment settings
#     arcpy.env.workspace = defineGDBpath(['aa','refinement'])
    
def featureToKML():

    in_data = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\aa\\sf\\combo_bfc_fc_ytc.shp'  
    in_field = "GRIDCODE"
    join_table = defineGDBpath(['aa','aa'])+'combo_bfc_fc_ytc_table'
    join_field = 'Value'
    fieldList = ["s9_ytc30_2008to2"]

    # Join two feature classes by the zonecode field and only carry 
    # over the land use and land cover fields
    arcpy.JoinField_management(in_data, in_field, join_table, join_field, fieldList)
  

    # # # create directories to hold kml file and associated images
    # stco_dir = rootpath + 'refinement/yfc/gridcode_' + wc + '/'
    # if not os.path.exists(stco_dir):
    #     os.makedirs(stco_dir)
    
    # #Set local variables
    # layer = "gridcode_" + wc
    # where_clause = "gridcode = " + wc 

    # # # Make a layer from the feature class
    # arcpy.MakeFeatureLayer_management(fc, layer, where_clause)

    # out_kmz_file =  stco_dir + 'gridcode_' + wc  + '.kmz'
    # arcpy.LayerToKML_conversion(layer, out_kmz_file)



##############  NEW CODE FOR GRAPHS ####################################################################

def transposeTable():
    # transpose count tables to create series-----------------------------------------
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query1 = 'SELECT * FROM counts.s9_ytc30_2008to2016_mmu5_msk_nbl where percent is not null'
    df = pd.read_sql_query(query1, con=engine)
    print df

    df = df.pivot(index='index', columns='value', values='acres')
    print df

    # table = 'core_pivot'
    # df.to_sql(table, engine, schema='refinement')




#### call functions #######
# transposeTable()
# featureToKML()

###############WORKFLOW______________________________________________________



def main_setUpGDB():

    states = gen.getStatesField("st_abbrev")
    for state in states:
        print state
        arcpy.CreateFileGDB_management(rootpath+'qaqc', state.lower()+".gdb")
        years = [2010,2011,2012,2013]
        for year in years:
            importRasterToGDB(state,year)





    def importRasterToGDB(state,year):

        matches = []
        for root, dirnames, filenames in os.walk('D:\\data\\CDL_confidence_layers_2006-2014\\'+str(year)):
            for filename in fnmatch.filter(filenames, '*_'+state+'_*.img'):
                print 'filename:', filename[:-4]
                matches.append(os.path.join(root, filename))
        print matches
        # arcpy.CheckOutExtension("Spatial")


        # Use try/except to skip the states that dont have all years
        try:
            arcpy.CopyRaster_management(matches[0],defineGDBpath(['qaqc',state])+filename[:-4],"DEFAULTS","0","","","","8_BIT_UNSIGNED")
        except:
            pass










def main_createFC():

    states = gen.getStatesField("st_abbrev")
    for state in states:
        if state == 'IA':
            print state
            pgTableToFC(state)
            years = [2010,2011,2012,2013]
            for year in years:
                zonalstatsTable(state,year)



### run zonal stats
def zonalstatsTable(state,year):
    arcpy.env.workspace = defineGDBpath(['qaqc',state.lower()])

    #define arguments
    sf = state.lower()
    zone_field = "LINK"
    in_value_raster = 'cdl_30m_r_'+state+'_'+str(year)+'_albers_confidence'
    out_table = sf+'_'+str(year)+'_conf'

    # Check out the ArcGIS Spatial Analyst extension license
    arcpy.CheckOutExtension("Spatial")

    # Execute ZonalStatisticsAsTable
    ZonalStatisticsAsTable(sf, zone_field, in_value_raster, out_table, "DATA", "ALL")

    gen.addGDBTable2postgres(['qaqc',state.lower()],'*_'+str(year)+'_*','qaqc')
    






def pgTableToFC(state):
    sf = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/qaqc/sf/'+state.lower()+'.shp'
    sf_acea = sf.replace(".shp", "_acea")

    command = 'pgsql2shp -f "'+sf+'" -h 144.92.235.105 -u mbougie -P Mend0ta! usxp "SELECT combo_bfc_fc_ytc_spatial.gid, combo_bfc_fc_ytc_spatial.gid::text as link, combo_bfc_fc_ytc.year, combo_bfc_fc_ytc_spatial.geom FROM qaqc.combo_bfc_fc_ytc, qaqc.combo_bfc_fc_ytc_spatial,spatial.states WHERE combo_bfc_fc_ytc_spatial.gridcode = combo_bfc_fc_ytc.value and st_within(combo_bfc_fc_ytc_spatial.geom, states.geom) and st_abbrev = \''+state+'\' "'
    print command
    os.system(command)

    ### reproject shapefile to acea  ---still need
    print 'define reprojection'
    sr = arcpy.SpatialReference(5070)
    print sr
    arcpy.DefineProjection_management(sf, sr)

    print 'reprojection'
    arcpy.Project_management(sf, defineGDBpath(['qaqc',state])+state.lower(), "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "", "PROJCS['NAD_1983_UTM_Zone_15N',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-93.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")









def testit():
    cur = conn.cursor()


    query = """Create Table qaqc.ia_merged as 
              SELECT 
              combo_bfc_fc.bfc, 
              combo_bfc_fc.fc, 
              combo_bfc_fc_ytc.value, 
              combo_bfc_fc_ytc.year,
              st_area(combo_bfc_fc_ytc_spatial.geom) * 0.000247105 as acres,
              ia_2010_conf.mean as conf_2010,
              ia_2011_conf.mean as conf_2011,
              ia_2012_conf.mean as conf_2012,
              ia_2013_conf.mean as conf_2013
            FROM 
              qaqc.combo_bfc_fc, 
              qaqc.combo_bfc_fc_ytc, 
              qaqc.combo_bfc_fc_ytc_spatial, 
              qaqc.ia_2010_conf,
              qaqc.ia_2011_conf,
              qaqc.ia_2012_conf,
              qaqc.ia_2013_conf
            WHERE 
              combo_bfc_fc.value = combo_bfc_fc_ytc.conv_traj AND
              combo_bfc_fc_ytc.value = combo_bfc_fc_ytc_spatial.gridcode AND
              combo_bfc_fc_ytc_spatial.gid::text = ia_2010_conf.link AND
              ia_2010_conf.link = ia_2011_conf.link AND
              ia_2011_conf.link = ia_2012_conf.link AND
              ia_2012_conf.link = ia_2013_conf.link
            order by acres desc"""


    print query
    cur.execute(query)
    conn.commit()
    # query.replace('ia_', 'mo_')
    # print query.replace('ia_', 'mo_')

    # query.replace('a', '%temp%').replace('b', 'a').replace('%temp%', 'b')
    







####################### call main functions ##########################################################################

### functions for confidence datasets
# main_setUpGDB()
# main_createFC()



#### functions to export shapefile 
testit()
