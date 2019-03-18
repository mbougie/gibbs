import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
# import general_deliverables as gen_dev


#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



try:
    conn = psycopg2.connect("dbname= 'usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')






def combineRasters(graphic_type, yxc, cdl_type):
    print 'combineRasters..................................................'
    states = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\misc.gdb\\states'
    yxc_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\s35.gdb\\s35_{}'.format(yxc)
    cdl_specific_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\s35.gdb\\s35_{}'.format(cdl_type)


    if graphic_type == 'sm':
        # output= 'D:\\projects\\usxp\\deliverables\\s35\\sm_specifics.gdb\\sm_state_yfc_bfnc'
        output= 'D:\\projects\\usxp\\deliverables\\s35\\conus.gdb\\s35_{}_sm'.format(cdl_type)
        print 'small multiples-----------------------------------'

        # ###Execute Combine
        outCombine = Combine([states, yxc_file, cdl_specific_file])

        ###Save the output 
        outCombine.save(output)

        ###create pyraminds
        gen.buildPyramids(output)

    elif graphic_type == 'conus':
        output= 'D:\\projects\\usxp\\deliverables\\s35\\conus.gdb\\s35_{}_conus'.format(cdl_type)
        print 'conus---------------------------------------------'

        # ###Execute Combine
        outCombine = Combine([yxc_file, cdl_specific_file])

        ###Save the output 
        outCombine.save(output)

        ###create pyraminds
        gen.buildPyramids(output)





def addRasterAttrib2postgres_specific(graphic_type, yxc, cdl_type, database, schema):
    filename = 's35_{0}_{1}'.format(cdl_type, graphic_type)
    print filename
    path='D:\\projects\\usxp\\deliverables\\s35\\conus.gdb\\{0}'.format(filename)
    print path

    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(database))
    
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(path)]
    print 'fields:', fields

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(path,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    if graphic_type == 'sm':
        df.columns = ['objectid','value','count','state','year','label']

        ##add 0 infront of state where length of state code is equal to 1
        df['state'] = np.where(df['state'].astype(str).str.len()==1, '0' + df['state'].astype(str), df['state'].astype(str))
        ## add acres column
        df['acres'] = df['count'] * gen.getPixelConversion2Acres(30)

        print 'df-----------------------', df

        # # # use pandas method to import table into psotgres
        df.to_sql(filename, engine, schema=schema, if_exists='replace')


    elif graphic_type == 'conus':
        print 'inside conus!!!!!!!!!!!!!!!!!!!!!!'
        df.columns = ['objectid','value','count','year','label']

        ## add acres column
        df['acres'] = df['count'] * gen.getPixelConversion2Acres(30)

        print 'df-----------------------', df

        # # # use pandas method to import table into psotgres
        df.to_sql(filename, engine, schema=schema, if_exists='replace')
    









def run():
    combineRasters('sm', 'yfc', 'fnc')
    addRasterAttrib2postgres_specific(graphic_type='conus', yxc='yfc', cdl_type='fnc', database='usxp', schema='combine')




#####  call main function  ###########################################################################
# if __name__ == '__main__':
#     print hi
    # run(instance, data)


# print fiona.supported_drivers

















