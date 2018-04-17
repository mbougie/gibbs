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
import subprocess as sp


'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


#################### class to create yxc object  ####################################################

class ProcessingObject(object):

    def __init__(self, series, res, mmu, years, name, subname):
        self.series = series
        self.res = str(res)
        self.mmu = str(mmu)
        self.years = years
        self.name = name
        print 'self.name', self.name 
        self.subname = subname
        print 'self.subname', self.subname 
        

        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        print 'self.datarange:', self.datarange
        
        self.conversionyears = range(self.years[0]+2, self.years[1])
        print 'self.conversionyears:', self.conversionyears
        
        # self.name_dataset = self.series+"_traj_cdl"+self.res+"_b_"+self.datarange+'_rfnd'
        # self.subname_dataset = self.series+"_traj_cdl"+self.res+"_b_"+self.datarange+'_rfnd_n8h_mtr_8w_msk'+self.mmu+'_nbl'
        # self.yxc_dataset = self.series+"_"+self.name+self.res+'_'+self.datarange
        # self.yxc_mmu_dataset = self.yxc_dataset+'_mmu'+self.mmu
        # self.yxc_mask_dataset = self.yxc_mmu_dataset+'_msk'
        
        # self.mmu_gdb=defineGDBpath(['core','mmu'])



def addGDBTable2postgres(gdb_args, raster, col_list):
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)
    
    
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
    df.to_sql(raster, engine, schema=gdb_args[0])

    #add trajectory field to table
    if 'acres' in col_list:
        addAcresField(raster, gdb_args[0])

    if 'traj_array' in col_list:
        print 'yo'
        addTrajArrayField(raster, fields, gdb_args[0])




def addTrajArrayField(tablename, fields, schema):

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



def addAcresField(tablename, schema):
    #this is a sub function for addGDBTable2postgres()
    print 'addAcresField'
    cur = conn.cursor()
    
    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN acres bigint;');
    
    #DML: insert values into new array column
    cur.execute('UPDATE '+ schema + '.' + tablename + ' SET acres = count * ' + str(gen.getPixelConversion2Acres(deliver.res)));
    
    conn.commit()
    print "Records created successfully";
    # conn.close()







#################  NLCD mask code  ############################################################################################################

def createSpecificLUCMask(table,column):
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(['ancillary', 'nlcd'])
    
    cond =  '*30_1992'
    print 'cond:', cond
    # print cond
    # #loop through each of the cdl rasters
    for raster in arcpy.ListDatasets(cond, "Raster"): 
        
        print 'raster: ',raster

        outraster = defineGDBpath(['deliverables', 'nlcd_masks'])+raster+'_'+column
        print 'outraster: ', outraster
       
        myRemapVal = RemapValue(getReclassifyValuesString(table,column))

        outReclassRV = Reclassify(raster, "VALUE", myRemapVal, "")

        # Save the output 
        outReclassRV.save(outraster)

        #create pyraminds
        gen.buildPyramids(outraster)



def getReclassifyValuesString(table,column):
    #Note: this is a aux function that the reclassifyRaster() function references
     
    cur = conn.cursor() 

    query = "SELECT value::text,nlcd_"+column+" FROM deliverables."+table
    print 'query----', query
    #DDL: add column to hold arrays
    cur.execute(query);
    
    #create empty list
    reclassifylist=[]

    # fetch all rows from table
    rows = cur.fetchall()
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        ww = [int(row[0]),(row[1])]
        reclassifylist.append(ww)
    
    print reclassifylist
    return reclassifylist






def getMaskBinaries():
    # dict_nlcd = {"pg_tables" : ["nlcd2001to2011","nlcd1992"], "columns" : ["planted","intact"]}
    dict_nlcd = {"pg_tables" : ["nlcd1992"], "columns" : ["planted","intact"]}
    for table in dict_nlcd.get("pg_tables"):
        for column in dict_nlcd.get("columns"):
            createSpecificLUCMask(table,column)



def createMasks(mask_type):
    arcpy.env.workspace = defineGDBpath(['deliverables', 'nlcd_masks'])
    
    cond = '*_'+mask_type
    rasters = arcpy.ListDatasets(cond, "Raster")
    print rasters
    print rasters[0]

    if mask_type == 'planted':
        #Logic: this is large footprint because its not selective.  Uses of the "or" operator.
        print 'planted'
        output = 'mask_plante3'
        print 'output:', output
        # outCon = Con(Raster(rasters[0]) == 1,1, Con(Raster(rasters[1]) == 1,1, Con(Raster(rasters[2]) == 1,1, Con(Raster(rasters[3]) == 1,1))))
        outCon = Con((Raster(rasters[0]) == 1) | (Raster(rasters[1]) == 1) | (Raster(rasters[2]) == 1) | (Raster(rasters[3]) == 1), 1)

    elif mask_type == 'intact':
        #Logic: this is smaller footprint because it's selective.  Use of the "and" operator.
        print 'intact'
        output = 'mask_intact'
        print 'output:', output
        outCon = Con(((Raster(rasters[0]) == 1) & (Raster(rasters[1]) == 1) & (Raster(rasters[2]) == 1) & (Raster(rasters[3]) == 1)), 1)

    outCon.save(output)

    gen.buildPyramids(output)


################################################################################################################################################


def subsetDataset():
    # Check out the ArcGIS Spatial Analyst extension license
    arcpy.CheckOutExtension("Spatial")
    
    ### define inputs
    raster_name = 's9_'+deliver.name+'30_2008to2016_mmu5_nbl'
    inRaster = defineGDBpath(['post', deliver.name])+raster_name
    print 'inRaster', inRaster
    
    false_name = raster_name+ '_'+deliver.subname+'_nbl'
    falseRaster = defineGDBpath(['post', deliver.name])+false_name
    print 'falseRaster', falseRaster

    ### define outputs
    # output_name = raster_name + '_ss'
    output_name = false_name + '_ss'
    print 'output_name:', output_name
    output = defineGDBpath(['deliverables', 'usxp']) + output_name
    
    ### define the condition
    cond = "Value < 2013"

    # outSetNull = SetNull(inRaster, inRaster, cond)
    outSetNull = SetNull(inRaster, falseRaster, cond)

    # # Save the output 
    outSetNull.save(output)  

    gen.buildPyramids(output)




    # addGDBTable2postgres(output_name)




def tryinport(table_initial, table_final):
    cur = conn.cursor()
    query = ("create table deliverables."+table_final+" as" 
            " SELECT "
            """ specific_luc.value, '"'||lookup_cdl.name||'"' as name, '"'||lookup_cdl.name||'"' as group_name, """
            " round(specific_luc.acres::numeric/1000000,2) as acres_mil, "
            " specific_luc.acres, "
            " (SELECT round(acres/(select sum(acres) from deliverables."+table_initial+") * 100,2)) as percent "
            " FROM "
            " deliverables."+table_initial+" as specific_luc, "
            " misc.lookup_cdl "

            "WHERE "
            "lookup_cdl.value = specific_luc.value "
            "order by acres desc;")
    print query
    # cur.execute(query)
    # conn.commit()



    if table_final == 'fc' or table_final == 'bfnc':
        query2 = ("""update deliverables."""+table_final+""" set group_name = '"'||'Other'||'"' where percent < (select percent from deliverables."""+table_final+" where value=4)")
        print query2
        cur.execute(query2)
        conn.commit()

        query3 = ("""update deliverables."""+table_final+""" set group_name = '"'||'Wheat'||'"'  where value=22 or value=23 or value=24 """)
        print query3
        cur.execute(query3)
        conn.commit()



    elif table_final == 'bfc' or table_final == 'fnc':
        query2 = ("""update deliverables."""+table_final+""" set group_name = '"'||'Other'||'"' where percent < (select percent from deliverables."""+table_final+" where value=152)")
        print query2
        cur.execute(query2)
        conn.commit()

        query3 = ("""update deliverables."""+table_final+""" set group_name = '"'||'Forest'||'"'  where value=141 or value=142 or value=143 """)
        print query3
        cur.execute(query3)
        conn.commit()

        query4 = ("""update deliverables."""+table_final+""" set group_name = '"'||'Wetland'||'"'  where value=190 or value=195 """)
        print query4
        cur.execute(query4)
        conn.commit()




def createBars(table_final): 
    cur = conn.cursor() 
    query = 'create table deliverables.'+table_final+'_bars as SELECT group_name, round(sum(acres::numeric/1000000),2) as acres_mil, sum(percent) as perc FROM deliverables.'+table_final+' group by group_name order by sum(percent) desc'
    print query
    cur.execute(query)
    conn.commit()






# pgsql2shp -f "C:/Users/Bougie/Desktop/Gibbs/data/usxp/aa/sf/testityo.shp" -h myserver -u apguser -P apgpassword mygisdb 
#     "SELECT neigh_name, the_geom FROM neighborhoods WHERE neigh_name = 'Jamaica Plain'"

################ Instantiate the class to create yxc object  ########################
deliver = ProcessingObject(
      #series
      's9',
      #resolution
      30,
      #mmu
      5,
      #data range---i.e. all the cdl years you are referencing 
      [2008,2016],
      #name
      'yfc',
      #subname
      'fnc'
      )











#######  call functions  #############################################
# getMaskBinaries()

# createMasks('planted')



# subsetDataset()


# createLegendData()

addGDBTable2postgres(['aa', 'aa'], 'combo_bfc_fc_ytc', ['acres', 'traj_array'])
# dict_tables = {'bfnc':'s9_yfc30_2008to2016_mmu5_nbl_bfnc_nbl_ss_5mmu', 'fnc':'s9_yfc30_2008to2016_mmu5_nbl_fnc_nbl_ss_5mmu', 'fc':'s9_ytc30_2008to2016_mmu5_nbl_fc_nbl_ss', 'bfc':'s9_ytc30_2008to2016_mmu5_nbl_bfc_nbl_ss'}
# # dict_tables = {'bfnc':'s9_yfc30_2008to2016_mmu5_nbl_bfnc_nbl_ss_5mmu'}
# for table_final, table_initial in dict_tables.iteritems():
#     # if table_final == 'bfc':
#     # tryinport(table_initial, table_final)
#     createBars(table_final)







# table2sf()

