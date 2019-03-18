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
    conn = psycopg2.connect("dbname= 'usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')






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
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name=eu_col, value_name="count")


    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df[eu_col] = df[eu_col].map(lambda x: x.strip('ATLAS_'))
    ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    print df


    print 'lkl', gen.getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*0.22227

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema='choropleths')

    # MergeWithGeom(df, tablename, eu, eu_col)






# def MergeWithGeom(df, tablename, eu, eu_col):

#     sql = "select atlas_name, {}, acres_calc, geom from spatial.{}".format(eu_col, eu)

#     df_spatial = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom' )

#     layer = pd.merge(df_spatial, df, on=eu_col)

#     layer['yxc_perc'] = (layer['acres']/layer['acres_calc'])*100
#     print layer

#     layer.to_file('D:\\projects\\usxp\\deliverables\\maps\\choropleths\\choropleths.gdb', layer=tablename, driver='FileGDB')











# def creatZonalTable(instance, inraster, out_table, eu_col):
#     #reclass mtr using relcasslist
#     outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
#     print 'finished Reclassify..............'
#     # outReclass.save("C:\\Users\\Bougie\\Desktop\\temp\\temp.gdb\\qaqc_reclass")

  

#     #use county as the zone
#     zonal_input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(instance['enumeration_unit'])
#     ZonalStatisticsAsTable(zonal_input, eu_col, inraster, out_table, "DATA", "SUM")
#     print 'ZonalStatisticsAsTable..............'
    




def createHistoTable(instance, inraster, out_table, zone_field):
    print 'createHistoTable..................................................'

    inZoneData = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(instance['enumeration_unit'])
    ZonalHistogram(inZoneData, zone_field, inraster, out_table)
    print 'ZonalHistogram-------------'
    


def convertPGtoFC(gdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update D:\\projects\\usxp\\deliverables\\maps\\choropleths\\{0}.gdb PG:"dbname=usxp_deliverables user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt MULTIPOLYGON'.format(gdb, schema, table)
    print command
    os.system(command)






def run(instance, data):

    eu_col = {'counties':'atlas_stco', 'states':'atlas_st'}

    inraster = Raster(data['core']['path'])
    print 'inraster', inraster


    if instance['enumeration_unit'] == 'counties':
        print 'counties-------------------------------------------------------------------'
        out_table = 'D:\\projects\\usxp\\deliverables\\maps\\choropleths\\choropleths.gdb\\test'
        # createHistoTable(instance, inraster, out_table, eu_col[instance['enumeration_unit']])
        addGDBTable2postgres_histo(out_table, instance['enumeration_unit'], eu_col[instance['enumeration_unit']])
    

    elif instance['enumeration_unit'] == 'states':
        print 'states-------------------------------------------------------------------'

        out_table = data['post'][yxc]['zonal_hist_path_fc']
        print 'out_table', out_table

        createHistoTable(instance, inraster, out_table, eu_col[instance['enumeration_unit']])
        addGDBTable2postgres_histo(out_table, instance['enumeration_unit'], eu_col[instance['enumeration_unit']])



#####  call main function  ###########################################################################
if __name__ == '__main__':

    # run(instance, data)

    ###run sql #################

    ###export postgres table to arcgis
    convertPGtoFC(gdb='choropleths', schema='choropleths', table='s35_perc_cov')

# print fiona.supported_drivers

















