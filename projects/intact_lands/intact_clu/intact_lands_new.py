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



# try:
#     conn = psycopg2.connect("dbname= 'usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
# except:
#     print "I am unable to connect to the database"

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')






def addGDBTable2postgres(currentobject):
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr

    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["objectid"],var_name="oid")
    print df

    df['oid'] = df['oid'].map(lambda x: x.strip('objec_'))


    tablename='zonal_nlcd_swift'
    schema = 'refine'

    df.to_sql(tablename, engine, schema=schema)
    

# layer.to_file("result.gdf",driver="FileGDB")


def tryit(df):

    # yo = pd.merge(df, df_spatial, on='atlas_stco')

    # df_spatial = pd.read_sql_query('select atlas_name, atlas_stco, acres_calc, geom from spatial.counties',con=engine)


    sql = "select atlas_name, atlas_stco, acres_calc, geom from spatial.counties"

    df_spatial = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geom' )

    layer = pd.merge(df_spatial, df, on='atlas_stco')

    layer['ytc_perc'] = (layer['acres']/layer['acres_calc'])*100
    print layer

    # df_spatial['ytc_perc'] = (df['acres']/df_spatial['acres_calc'])*100

    # print df_spatial


    # layer = pd.merge(df_spatial, df, on='atlas_stco')

    # print layer


# filename=temp_shp,driver='ESRI Shapefile'

    # out = 'C:\\Users\\Bougie\\Desktop\\temp\\temp.gdb\\test'
    # layer.to_file("result.gdb", layer="layer_name", driver="FileGDB")
    layer.to_file('C:\\Users\\Bougie\\Desktop\\temp\\temp.gdb', layer='meow_t12', driver='FileGDB')

    # layer.to_file(filename='hi',driver="FileGDB")

    # df.to_file('MyGeometries.shp', driver='ESRI Shapefile')










def processingCluster(instance, inraster, out_table, reclasslist):
    #reclass mtr using relcasslist
    outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
    print 'finished Reclassify..............'
    # outReclass.save("C:\\Users\\Bougie\\Desktop\\temp\\temp.gdb\\qaqc_reclass")

  

    #use county as the zone
    zonal_input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties'
    ZonalStatisticsAsTable(zonal_input, "atlas_stco", outReclass, out_table, "DATA", "SUM")
    print 'ZonalStatisticsAsTable..............'
    












def run_mtr(instance):
    inraster = Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_mtr'.format(instance['series']))
    print 'inraster', inraster


    for mtr, reclasslist in instance['yxc'].iteritems():

        out_table = 'D:\\projects\\usxp\\deliverables\\{0}\\maps\\choropleths\\usxp\\usxp.gdb\\{0}_{1}_counties'.format(instance['series'], mtr)
        print 'out_table', out_table

        processingCluster(instance, inraster, out_table, reclasslist)



def run_yxc(instance):

    for yxc, reclasslist in instance['yxc'].iteritems():

        inraster = Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_{1}'.format(instance['series'], yxc))
        print 'inraster', inraster

        out_table = 'D:\\projects\\usxp\\deliverables\\{0}\\maps\\choropleths\\usxp\\usxp.gdb\\{0}_{1}_2009to2012_counties_t2'.format(instance['series'], yxc)
        print 'out_table', out_table

        # processingCluster(instance, inraster, out_table, reclasslist)

        addGDBTable2postgres(out_table)







def newyo():
    try:
        conn = psycopg2.connect("dbname= 'intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
    except:
        print "I am unable to connect to the database"
    
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')

    sql1 = "SELECT gid, geom FROM spatial.clu_2015_noncrop_c_swift_mask_road25m_seperate"

    df_spatial = gpd.GeoDataFrame.from_postgis(sql1, conn, geom_col='geom' )


    # df_spatial = gpd.read_file("D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_mitchell\\final.gdb", layer="mitchell_w_roads25m_seperate", driver="FileGDB")
    print df_spatial

    sql2 ="""SELECT 
              oid::integer, 
              sum(value) as dev_pixels,
              total_pixels,
              round((sum(value)::numeric/total_pixels::numeric),2) as perc
            FROM 
              refine.zonal_nlcd_swift as a INNER JOIN (select oid, sum(value) as total_pixels from refine.zonal_nlcd_swift group by oid) as b using(oid)
              
            where value > 0 AND objectid IN (21, 22, 23, 24)

            group by oid, total_pixels"""

    df = pd.read_sql(sql2, engine)

    print df

    layer = pd.merge(df_spatial, df, left_on='gid', right_on='oid')

    print 'layer', layer

    # layer['ytc_perc'] = (layer['acres']/layer['acres_calc'])*100
    # print layer

    # # df_spatial['ytc_perc'] = (df['acres']/df_spatial['acres_calc'])*100

    # # print df_spatial


    # # layer = pd.merge(df_spatial, df, on='atlas_stco')

    # # print layer


    # # filename=temp_shp,driver='ESRI Shapefile'

    # # out = 'C:\\Users\\Bougie\\Desktop\\temp\\temp.gdb\\test'
    # # layer.to_file("result.gdb", layer="layer_name", driver="FileGDB")
    layer.to_file('D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\final.gdb', layer='zonal_nlcd_swift', driver='FileGDB')














#####  call main function  ###########################################################################
# instance = {'series':'s27', 'yxc':{'mtr3':[[3,1]], 'mtr4':[[4,1]]} }
# run_mtr(instance)



#####  call main function  ###########################################################################
# instance = {'series':'s27', 'yxc':{'ytc':[[2009,1], [2010,1], [2011,1], [2012,1]]} }
# run_yxc(instance)


# addGDBTable2postgres('D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\urban.gdb\\zonal_nlcd_swift')
newyo()






print fiona.supported_drivers

















