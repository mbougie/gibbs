from sqlalchemy import create_engine
import numpy as np, sys, os
from osgeo import gdal
from osgeo.gdalconst import *
# from pandas import read_sql_query
import pandas as pd
# import tables
import collections
from collections import namedtuple
import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2


production_type='production'
arcpy.CheckOutExtension("Spatial")
env.scratchWorkspace ="C:/Users/bougie/Documents/ArcGIS/scratch.gdb"


def CountsCDL():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/refinement')
    dir = 'C:/Users/bougie/Desktop/gibbs/refinement/minn'
    os.chdir(dir)
    for file in glob.glob("*.csv"):
        print(file)
        table=file[:-7]
        print 'table: ',table
        df = pd.read_csv(file)
        df2 = df.rename(columns={' Count': table})
        df2.to_sql(table, engine, schema='minn')





def getCellValue():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/refinement')
    dir = 'C:/Users/bougie/Desktop/gibbs/refinement/CDLs_de_biased/By_state/de_biased_2008'
    os.chdir(dir)
    for file in glob.glob("NASS_CDL_ME08_accuracy.xlsx"):
        print(file)
        table=file[:-7]
        print 'table: ',table
        df = pd.read_excel(file, sheetname='Summary Table')
        sub_df = df.tail(3)
        print sub_df
        crop_corrected = sub_df.iloc[1]['Unnamed: 16']
        print crop_corrected
        
        # df2 = df.rename(columns={' Count': table})
        # df2.to_sql(table, engine, schema='minn')



def gdbTable2postgres(table):

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')

    input = "C:/Users/bougie/Desktop/gibbs/refinement/refinement.gdb/"+table
    arr = arcpy.da.TableToNumPyArray(input, ('ATLAS_STCO', 'VALUE_3'))
    print arr



    df = pd.DataFrame(data=arr)
    print df


    df.to_sql(table, engine, schema='counts')



def csv2pg():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')
    dir = 'C:/Users/bougie/Desktop/gibbs/refinement/crp'
    os.chdir(dir)
    for file in glob.glob("*.xls"):
        print(file)
        print 'file: ', file
        table=file[:-7]
        print 'table: ', table

        df = pd.read_excel(file, sheetname='2013', header=None, skiprows=5, skip_footer=8)
        
        df1 = df[[0,1,2,3,11]]
        df1.columns = ['state', 'county', 'fips', 'total_crp', 'removed']
        print df1

        df1.to_sql('crp_2013', engine, schema='refinement')



def clipRastersByPolygon():
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/refinement/refinement.gdb'
    for raster in arcpy.ListFeatureClasses('mmu23*'):
        print raster


        







############  call functions  ############################################
# getCellValue()
gdbTable2postgres('traj_n8h_mtr_8w_msk68_ta')
# csv2pg()
# clipRastersByPolygon()

