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









def convertFCtoPG(gdb, pgdb, schema, table, geomtype, epsg):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname=intactland user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nlt PROMOTE_TO_MULTI -nln {1}.{2} {2} -progress --config PG_USE_COPY YES'.format(gdb, schema, table)
    
    os.system(command)

    gen.alterGeomSRID(pgdb, schema, table, geomtype, epsg)



def convertPGtoFC(gdb, pgdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update {0} PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -nln {3} -nlt MULTIPOLYGON'.format(gdb, pgdb, schema, table)
    print command
    os.system(command)





def createBufferPGtable():
    sql = '''CREATE TABLE roads_buff25.roads_buff25_{1}_no_dissolve as 
        SELECT clipped.atlas_name, ST_Buffer(ST_SnapToGrid(clipped_geom,0.0001),25) as geom
        FROM 
        (SELECT counties.atlas_name, (ST_Dump(ST_Intersection(counties.wkb_geometry, roads.wkb_geometry))).geom As clipped_geom
        FROM spatial.states_102003 as counties
        INNER JOIN refine.region_roads_102003 as roads
        ON ST_Intersects(counties.wkb_geometry, roads.wkb_geometry)
        )  As clipped

        WHERE ST_Dimension(clipped.clipped_geom) = 1 and clipped.atlas_name = '{0}';



    -----create spatial index-----------------
    CREATE INDEX roads_buff25_{1}_no_dissolve_geom_idx
    ON roads_buff25.roads_buff25_{1}_no_dissolve 
    USING gist
    (geom);'''.format(key, value)


    print sql
    gen.postgres_ddl(pgdb='intactland', sql=sql)









#####  call main function  ###########################################################################
# instance = {'series':'s27', 'yxc':{'mtr3':[[3,1]], 'mtr4':[[4,1]]} }
# run_mtr(instance)



#####  call main function  ###########################################################################
# instance = {'series':'s27', 'yxc':{'ytc':[[2009,1], [2010,1], [2011,1], [2012,1]]} }
# run_yxc(instance)


# addGDBTable2postgres('D:\\projects\\intact_land\\intact\\refine\\test_areas\\mn_swift\\urban.gdb\\zonal_nlcd_swift')



# convertFCtoPG(gdb='I:\\d_drive\\projects\\intactland\\intact_clu\\refine\\masks\\development\\transport\\transport.gdb', pgdb='intactland', schema='refine', table='region_roads_102003', geomtype='MultiLineString', epsg=102003)


# convertFCtoPG(gdb='D:\\intactland\\intact_clu\\refine\\masks\\development\\urban\\urban.gdb', pgdb='intactland', schema='refine', table='region_cdl_2015_dev_5mmu_102003', geomtype='MULTIPOLYGON', epsg=102003)


# convertFCtoPG(gdb='E:\\data\\shapefiles.gdb', pgdb='intactland', schema='spatial', table='states_102003', geomtype='MULTIPOLYGON', epsg=102003)


# convertPGtoFC(gdb='I:\\d_drive\\projects\\intactland\\intact_clu\\test2.gdb', pgdb='intactland', schema='test', table='roads_buff25_iowa_no_dissolve')


###################################################################
###### Raster code ################################################
###################################################################

###### roads_buff25 ###############################################
region_states = {'Minnesota':'minnesota', 'Iowa':'iowa', 'North Dakota':'northdakota', 'South Dakota':'southdakota', 'Montana':'montana', 'Nebraska':'nebraska', 'Wyoming':'wyoming'}
# region_states = {'Montana':'montana', 'Nebraska':'nebraska'}

# for key, value in region_states.items():    # for name, age in dictionary.iteritems():  (for Python 2.x)
#     print key
#     print value

    ##### create the buffer tables by state with postgres SQL query ######################
    # createBufferPGtable()
    
    #### export buffer table in postgres to gdb ###################################################################
    # convertPGtoFC(gdb='D:\\intactland\\intact_clu\\refine\\masks\\development\\transport\\roads_buff25.gdb', pgdb='intactland', schema='roads_buff25', table='roads_buff25_{}_no_dissolve'.format(value))

    ##### convert polygon to raster #####################################
    ### set the environment for the new raster ####
    # env.workspace = 'D:\\intactland\\intact_clu\\refine\\masks\\development\\transport\\roads_buff25.gdb'
    # clu_2015_noncrop_c_30m = Raster('D:\\intactland\\intact_clu\\main\\years\\2015_initial.gdb\\clu_2015_noncrop_c_30m')
    # arcpy.env.snapRaster = clu_2015_noncrop_c_30m
    # arcpy.env.cellsize = clu_2015_noncrop_c_30m
    # arcpy.env.outputCoordinateSystem = clu_2015_noncrop_c_30m

    # arcpy.PolygonToRaster_conversion(in_features='roads_buff25_{}_no_dissolve'.format(value), value_field='OBJECTID', out_rasterdataset='roads_buff25_{}_no_dissolve_30m'.format(value), cell_assignment='CELL_CENTER', cellsize=30)






#### mosaic state road_buf25 rasters together.
# env.workspace = 'D:\\intactland\\intact_clu\\refine\\masks\\development\\transport\\roads_buff25.gdb'
# input_rasters = arcpy.ListRasters("*", "GRID")
# print input_rasters
# arcpy.MosaicToNewRaster_management(input_rasters=input_rasters, output_location='D:\\intactland\\intact_clu\\refine\\masks\\development\\transport\\transport.gdb', raster_dataset_name_with_extension='region_roads_buff25_30m', pixel_type='32_BIT_UNSIGNED', cellsize=30, number_of_bands=1)



### reclass rasters to binary #############
# env.workspace = 'D:\\intactland\\intact_clu\\refine\\masks\\development\\urban\\urban.gdb'
# input_rasters = arcpy.ListRasters("*", "GRID")
# print input_rasters
# for raster in input_rasters:
#     print raster
#     Ras = Con(IsNull(raster), 1, 0)  
#     Ras.save('{}_b'.format(raster))  



##### footprint masks ####################################
# Execute PolygonToRaster

####set environment variables #####################################################
clu_2015_noncrop_c_30m = Raster('D:\\intactland\\intact_clu\\main\\years\\2015_initial.gdb\\clu_2015_noncrop_c_30m')
arcpy.env.snapRaster = clu_2015_noncrop_c_30m
arcpy.env.cellsize = clu_2015_noncrop_c_30m
arcpy.env.outputCoordinateSystem = clu_2015_noncrop_c_30m
arcpy.env.extent = clu_2015_noncrop_c_30m.extent

####create year list 
yearlist = range(2003,2015) 
##remove 2010 because no data for that year
yearlist.remove(2010)

# yearlist = [2015]

# for year in yearlist:
#     year = str(year)
#     print 'year', year
#     env.workspace = 'D:\\intactland\\intact_clu\\main\\years\\{}_initial.gdb'.format(year)

#     arcpy.PolygonToRaster_conversion(in_features='clu_{}'.format(year), value_field='OBJECTID', out_rasterdataset='clu_{}_raster_extent'.format(year), cell_assignment='CELL_CENTER', cellsize=30)

 
##### mosiac datasets together   
###performed this step in gui


#####convert dataset to binary 0/1 (1 = footprint/0 = non-footprint)
env.workspace = 'D:\\intactland\\intact_clu\\refine\\masks\\merged\\merged.gdb'
outCon = Con(IsNull('mask_footprint_11'), 0, 1)
outCon.save('mask_footprint_11_b')


gen.buildPyramids('mask_footprint_11_b')
    





