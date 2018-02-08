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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import shutil
import matplotlib.pyplot as plt



#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



def rasterToPoly(data, gdb):
  print '#############  create shapefiles  #################################'
  #####  Set environment settings
  # stage = 'core'
  env.workspace = gdb
  print env.workspace

  route = data['core']['route']
  instance = data['global']['instance']
  fltr = data['core']['filter']
  rg = data['core']['rg']
  mmu = data['core']['mmu']

  vector_dir = data['vectors']
  vector = '{}\\{}_{}_{}_mmu{}.shp'.format(vector_dir, instance, fltr, rg, mmu)
  print 'vector:', vector

  rasters = arcpy.ListRasters("*", "All")
  for raster in rasters:
    print 'raster:', raster

    field = "VALUE"

    ####  Execute RasterToPolygon
    arcpy.RasterToPolygon_conversion(raster, vector, "NO_SIMPLIFY", field)
    
    #### create dissolved kml
    createDissolved(vector)

    # get2012Data(vector, data)

    # exportFCtoKML(vector)
  ziproot = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\zip'
  zipname = '{}\\{}_{}_{}_mmu{}'.format(ziproot, instance, fltr, rg, mmu)
  print zipname
  shutil.make_archive(zipname, 'zip', vector_dir)

    


def createDissolved(vector_input):
    print '#############  create dissolved vector  #################################'
    layer = "templayer"
    where_clause = "GRIDCODE = 3"
    vector_dissolved = vector_input.replace('.shp', '_dissolved.shp')

    #### create layer in order to subset the data
    arcpy.MakeFeatureLayer_management(vector_input, layer, where_clause)
    #### Dissolve_management (in_features, out_feature_class, {dissolve_field}, {statistics_fields}, {multi_part}, {unsplit_lines})
    arcpy.Dissolve_management(layer, vector_dissolved, ["GRIDCODE"], "", "MULTI_PART", "DISSOLVE_LINES")
    #### Delete layer after use
    arcpy.Delete_management(layer)

    ###create a kml from dissolved vector
    # exportFCtoKML(vector_input)


def get2012Data(vector_input, data):
    print '#############  create dissolved vector  #################################'
    layer = "templayer"
    where_clause = "GRIDCODE = 2012"
    vector_dissolved = vector_input.replace('.shp', '_dissolved.shp')

    #### create layer in order to subset the data
    arcpy.MakeFeatureLayer_management(vector_input, layer, where_clause)

    # SpatialJoin_analysis (target_features, join_features, out_feature_class, {join_operation}, {join_type}, {field_mapping}, {match_option}, {search_radius}, {distance_field_name})
    
    arcpy.SpatialJoin_analysis(target_features=layer, join_features=data['ancillary']['vector']['shapefiles']['counties_subset'], out_feature_class='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\temp\\tryit.skp', match_option='COMPLETELY_WITHIN')
    #### Delete layer after use
    arcpy.Delete_management(layer)

    ###create a kml from dissolved vector
    # exportFCtoKML(vector_input)



def exportFCtoKML(vector):
  print '#############  create kmz  #################################'
  layer = "templayer"
  vector_kml = vector.replace('.shp', '.kmz')
  where_clause = "GRIDCODE = 3"
  arcpy.MakeFeatureLayer_management(vector, layer, where_clause)
  # arcpy.MakeFeatureLayer_management(vector, layer)
  arcpy.LayerToKML_conversion(layer, vector_kml)
  
  #### Delete layer after use
  arcpy.Delete_management(layer)








####################  regression  ###################################################
def addGDBTable2postgres(data):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = data['post']['ytc']['path']

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df
    
    # # # use pandas method to import table into psotgres
    df.to_sql(data['post']['ytc']['filename'], engine, schema='sa')
    
    # #add trajectory field to table
    addAcresField(data['post']['ytc']['filename'], 'sa', data['global']['res'])




def addAcresField(tablename, schema, res):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    ####DDL: add column to hold arrays
    query1 = 'ALTER TABLE {}.{} ADD COLUMN acres bigint'.format(schema, tablename, tablename)
    print query1
    cur.execute(query1)
    conn.commit()

 

    #####DML: insert values into new array column
    cur.execute('UPDATE {}.{} SET acres = count * {}'.format(schema, tablename, gen.getPixelConversion2Acres(res)))
    conn.commit() 

    cur.execute('ALTER TABLE sa.acres_ytc ADD COLUMN {} bigint'.format(tablename));
    conn.commit()
    

    query2 = 'update sa.acres_ytc set {} = instance.acres from (select value, acres from sa.{}) as instance where acres_ytc.value = instance.value'.format(tablename, tablename)
    cur.execute(query2);
    conn.commit()

    query3 = 'DROP TABLE sa.{}'.format(tablename)
    cur.execute(query3);
    conn.commit()


def createGraph():
  engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
  # query = "SELECT * FROM sa.acres_ytc"
  query = """
          SELECT 
            s6_ytc30_2008to2016_mmu5_nbl.value as years, 
            s6_ytc30_2008to2016_mmu5_nbl.acres as s6, 
            s14_ytc30_2008to2016_mmu5_nbl.acres as s14
          FROM 
            counts.s6_ytc30_2008to2016_mmu5_nbl FULL OUTER JOIN
            counts.s14_ytc30_2008to2016_mmu5_nbl
          ON 
            s6_ytc30_2008to2016_mmu5_nbl.value = s14_ytc30_2008to2016_mmu5_nbl.value;
          """
  df = pd.read_sql_query(query, engine)


  print df

  df.plot(kind='line', colormap='summer')
  
  df.set_index('years').plot.line(rot=0, title='Conversion to Cropland (acres)', figsize=(15,10), fontsize=12)
  # df.set_index('value').plot.line(rot=0, title='Conversion to Cropland (acres)', figsize=(15,10), fontsize=12)
  ax = plt.gca()
  # ax.legend_.remove()
  ax.get_xaxis().get_major_formatter().set_useOffset(False)
  # ax.set_color_cycle(['red', 'green'])
  leg = plt.legend()

  for line in ax.get_lines():
    line.set_linewidth(1.5)

  for line in leg.get_lines():
    line.set_linewidth(1.5)

    # line.set_color('blue')

  plt.savefig("C:\\Users\\Bougie\\Desktop\\Gibbs\\pdf\\s6_s14.pdf", bbox_inches='tight')







def addGDBTable2postgres_temp():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\r2_4\\post\\ytc_r2_4.gdb\\ytc_r2_4_zonalhist"

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    #only select the row with year = 2012
    # df = df.loc[df['label'] == '2,012']
  
    ##delete the onbjectid column
    del df['objectid']
    df.rename(columns={'label': 'county', '2,012': 'count'}, inplace=True)
    # print df
    # reset the index to the label column nad then transpose the dataset
    # df = df.set_index('county').transpose()

    # df['county'] = df['county'].map(lambda x: x.lstrip('altas_'))
 
    df.to_sql('ytc_r2_4_zonalhist2', engine, schema='sa')
    

def addGDBTable2postgres_temp2():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\temp\\temp.gdb\\try_table_hist2"

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

 
    df.to_sql('ytc_r2_4_zonalhist3', engine, schema='sa')





def addGDBTable2postgres_now():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    # tablename = 'traj_'+wc
    # path to the table you want to import into postgres
    input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s16\\post\\ytc_s16.gdb\\s16_traj_years'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df
    
    # # # use pandas method to import table into psotgres
    df.to_sql('s16_traj_years', engine, schema='counts')
    
    # #add trajectory field to table
    addAcresField_now('s16_traj_years', 'counts', '30')







def addAcresField_now(tablename, schema, res):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    ####DDL: add column to hold arrays
    query1 = 'ALTER TABLE {}.{} ADD COLUMN acres bigint'.format(schema, tablename, tablename)
    print query1
    cur.execute(query1)
    conn.commit()

 

    #####DML: insert values into new array column
    cur.execute('UPDATE {}.{} SET acres = count * {}'.format(schema, tablename, gen.getPixelConversion2Acres(res)))
    conn.commit() 
# addGDBTable2postgres_temp2()

# addGDBTable2postgres_temp()
# addGDBTable2postgres_temp2()
# createGraph()

# addGDBTable2postgres_now()

