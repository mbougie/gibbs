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








##call functions
# addGDBTable2postgres()
# clipByMMUmask()
getIntersectionofRasters()