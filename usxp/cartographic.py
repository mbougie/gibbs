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




def transposeTableyo(gdb_args,wc):
    print 'transposeTableyo() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)

    for table in arcpy.ListTables(wc): 
        # print 'table: ', table
# 
        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(table)]
        
        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(table,fields)
        # print arr
        
        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        df = df.set_index('OBJECTID',inplace=True)
        
        print df

        # df.columns = df.iloc[0]
        df.reindex(df.index.drop(1))
        

        # df.columns = map(str.lower, df.columns)
        
        # use pandas method to import table into psotgres
        # df.to_sql(table, engine, schema='test')
        
        ####  nice 2 step solution  ####
        df = df.transpose()
        print df
        # df =df.idxmax(axis=1)
        # print df

        # df.to_sql(table, engine, schema='test')




def getcolumns():
	#Note: this is a aux function that the reclassifyRaster() function references
	cur = conn.cursor()

	query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test' AND table_name   = 'hex_hist'"
	#DDL: add column to hold arrays
	cur.execute(query);

	# fetch all rows from table
	rows = cur.fetchall()
	rows_trim = rows[2:]

	# interate through rows tuple to format the values into an array that is is then appended to the reclassifyli
	querylist = []
	for row in rows_trim:
		print row[0]
		querylist.append('"'+row[0]+'"')
	print querylist
	querystring = ','.join(querylist)
	print querystring


	query2 = 'SELECT hex_hist.index, GREATEST('+querystring+') AS max FROM test.hex_hist'
	print query2

	#DML: insert values into new array column
	cur.execute(query2);

	rows2 = cur.fetchall()
	print rows2
	# conn.commit()
	# print "Records created successfully";
	# conn.close()
    #     ww = [row[0] + ' ' + row[1]]
    #     reclassifylist.append(ww)
    
    # #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
    # columnList = ';'.join(sum(reclassifylist, []))
    # print columnList
    
    # #return list to reclassifyRaster() fct
    # return columnList






def clipByMMUmask():
    root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
    rasterlist = glob.glob(root_in+"*.tif")

    for raster in rasterlist:
        print raster

        output = raster.replace('.', '_mask.')
        print output

        # raster = + 'tile_4.tif'
        # print 'raster: ', raster

        # for count in masks_list:
        cond = "Count < " + str(gen.getPixelCount('30', 5))
        print 'cond: ',cond

        outSetNull = SetNull(raster, 1, cond)

        # Save the output 
        outSetNull.save(output)

        # gen.buildPyramids(output)





def mosiacRasters():
    root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
    tilelist = glob.glob(root_in+"*mask.tif")
    print tilelist 
    ######mosiac tiles together into a new raster
    nbl_raster = nibble.raster_name + '_8w'
    print 'nbl_raster: ', nbl_raster

    arcpy.MosaicToNewRaster_management(tilelist, nibble.gdb_path, nbl_raster, Raster(nibble.in_raster).spatialReference, nibble.pixel_type, nibble.res, "1", "LAST","FIRST")


def mask():
    mtr = Raster(defineGDBpath(['core','mmu'])+'traj_cdl30_b_2008to2016_rfnd_n8h_mtr_8w_msk5_nbl')
    ytc = Raster(defineGDBpath(['post','ytc'])+'ytc30_2008to2016_mmu5')
    outCon = Con((mtr == 3) & (IsNull(ytc)), 3, Con((mtr == 3) & (ytc >= 2008), ytc))
    output = defineGDBpath(['post','ytc'])+'ytc30_2008to2016_mmu5_msk'

    
    outCon.save(output)

    gen.buildPyramids(output)



#####call functions
print (sys.version)

# mask()
# transposeTableyo(['ancillary','temp'],'hex_hist')

# getcolumns()

# clipByMMUmask()