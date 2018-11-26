import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
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
    conn = psycopg2.connect("dbname= 'ksu_v3' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu_v3')






def addGDBTable2postgres(currentobject, eu, eu_col):
    print currentobject
  
    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)


    print 'lkl', gen.getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*0.22227

    print 'df-----------------------', df

    schema = 'deliverables'
    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    tryit(df, tablename, eu, eu_col)





def addGDBTable2postgres_histo(currentobject, eu, eu_col):
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    #r## remove column
    del df['OBJECTID']
    print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['atlas_st'] = df['atlas_st'].map(lambda x: x.strip('atlas_'))
    ## remove comma from year
    df['year'] = df['label'].str.replace(',', '')

    print df


    print 'lkl', gen.getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*0.22227

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema='zonal_hist')

    # MergeWithGeom(df, tablename, eu, eu_col)






def MergeWithGeom():

    sql = "SELECT * FROM merged.ksu_samples"

    df_spatial = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom' )

    # layer = pd.merge(df_spatial, df, on=eu_col)

    # layer['yxc_perc'] = (layer['acres']/layer['acres_calc'])*100
    # print layer

    df_spatial.to_file('E:\\ksu\\test.gdb', layer='ksu_samples', driver='FileGDB')




MergeWithGeom()