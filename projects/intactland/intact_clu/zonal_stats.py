
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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import shutil
import matplotlib.pyplot as plt
#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


# set the engine.....
engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')





def changeTableFormat(hist_table, table_name):
  fields = [f.name for f in arcpy.ListFields(hist_table)]

  # converts a table to NumPy structured array.
  arr = arcpy.da.TableToNumPyArray(hist_table,fields)
  
  # convert numpy array to pandas dataframe
  df = pd.DataFrame(data=arr)
  #r## remove column
  del df['OBJECTID']
  print df

  ##perform a psuedo pivot table
  df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_stco", value_name="count")

  ##convert all column values to lowercase
  df.columns = map(str.lower, df.columns)
    
  #### format column in df #########################
  ## strip character string off all cells in column
  df['atlas_stco'] = df['atlas_stco'].map(lambda x: x.strip('atlas_'))
  ## add zero infront of string if length is less than 2
  df['atlas_stco'] = df['atlas_stco'].apply(lambda x: '{0:0>2}'.format(x))

  #### add columns to table ########################
  # df['yxc'] = yxc
  df['acres'] = gen.getAcres(df['count'], 30)
  # df['year'] = year

  #### join tables to aquire the state abreviation #########
  df = pd.merge(df, pd.read_sql_query('SELECT atlas_stco,atlas_name,acres_calc FROM spatial.counties;',con=engine), on='atlas_stco')

  df['percent']=(df.acres/df.acres_calc)*100
  print df

  df.to_sql(table_name, engine, schema='counts', if_exists='replace')




if __name__ == '__main__':

  ###define parameters
  inZoneData='D:\\projects\\intact_land\\shapefiles\\counties_nd.shp'
  zoneField='atlas_stco'
  gdb='D:\\projects\\intact_land\\disturbed_subgroups.gdb\\'
  table_name='abandoned_nd'
  inValueRaster = '{}{}'.format(gdb,table_name)
  hist_table = '{}_hist'.format(inValueRaster)

  ##call zonal hsitgram function
  ZonalHistogram(inZoneData, zoneField, inValueRaster, hist_table)

  ##change zonal hist table and import in PG
  changeTableFormat(hist_table, table_name)


