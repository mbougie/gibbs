import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def addGDBTable2postgres(gdb,wc,pg_shema):
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = gdb

    for table in arcpy.ListTables(wc): 
        print 'table: ', table

        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(table)]

        print 'fields', fields[2:]
        
        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(table,fields)
        print arr
        
        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        df['label'] = 'cy_' + df['label'].str.replace(',', '')

        #### first melt(unpivot) the tile columns into one column
        df = pd.melt(df, id_vars=['label'], value_vars=fields[2:])

        ##THEN are able to pivot the new df once the do melt
        df = df.pivot(index='variable', columns='label', values='value')

        # print df['label']

        "cleans up the header of the dataframe so not stacked weird"
        del df.index.name
        
        print df.index
        # df['label'] = df.index
        # df.reset_index(level=0, inplace=True)
        # df['label'] = df.index

        print list(df.columns.values)

        # df['label'] = 'cy_' + df['label'].str.replace('oid_','')
        # df['label'].str.replace('oid_','')


        # df['variable'] = df['variable'].str.replace('oid_', '')
        print df
        # df['total'] = df.sum(axis=1)
        # print df

        df = ((df.T / (df.sum(axis=1)))*100).T

        print df


        df = pd.melt(df, id_vars=['label'], value_vars=fields[2:])

        # print df['variable']




        # print pd.melt(df, id_vars=['A'], value_vars=['B'])

        # print pd.melt(df, id_vars=['label'], value_vars=['fid_4989'])


        # print pd.melt(df, id_vars=['value'], var_name="Date", value_name="Value")

        # print df.unstack()
        
        
        # use pandas method to import table into psotgres
        df.to_sql(table, engine, schema=pg_shema, if_exists='replace')


        # 1752/5215

# addGDBTable2postgres('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\data_2008_2012.gdb', 'multitemporal_results_ff2_table', 'deliverables')


addGDBTable2postgres('D:\\projects\\usxp\\current_deliverable\\5_23_18\\deliverables.gdb', 'zh_fishnet_mtr', 'deliverables')